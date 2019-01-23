package client;

import client.exceptions.*;
import common.CorrelatedMessage;
import common.hash.HashRing;
import common.hash.NodeEntry;
import common.hash.Range;
import common.messages.DefaultKVMessage;
import common.messages.KVMessage;
import common.messages.mapreduce.InitiateMRRequest;
import common.messages.mapreduce.InitiateMRResponse;
import common.messages.mapreduce.MRStatusMessage;
import common.messages.mapreduce.MRStatusRequest;
import common.utils.HostAndPort;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KVStore implements KVCommInterface {

    private static final Logger LOG = LogManager.getLogger(KVStore.class);

    private final HashRing hashRing;
    private final Map<HostAndPort, CommunicationModule> communicationModules;
    private boolean running;
    private final HostAndPort seedAddress;

    /**
     * Initialize KVStore with address and port of KVServer
     *
     * @param address the address of the seed KVServer
     * @param port    the port of the seed KVServer
     */
    public KVStore(String address, int port) {
        this.hashRing = new HashRing();
        this.communicationModules = new ConcurrentHashMap<>();
        this.seedAddress = new HostAndPort(address, port);
        this.running = false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void connect() throws ClientException {
        if (!communicationModules.containsKey(seedAddress)) {
            addNodeConnection(seedAddress);
        }
        for (CommunicationModule communicationModule : communicationModules.values()) {
            communicationModule.start();
        }
        this.running = true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void disconnect() {
        for (CommunicationModule communicationModule : communicationModules.values()) {
            communicationModule.stop();
        }
        this.running = false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KVMessage put(String key, String value) throws ClientException {
        ensureConnected();

        // not my preferred API, but we want to match the interface
        if (value == null || "null".equals(value)) {
            return delete(key);
        }

        KVMessage outgoing = new DefaultKVMessage(key, value, KVMessage.StatusType.PUT);
        KVMessage reply = sendAndGetReply(outgoing);

        return reply;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KVMessage get(String key) throws ClientException {
        ensureConnected();

        KVMessage outgoing = new DefaultKVMessage(key, null, KVMessage.StatusType.GET);
        KVMessage reply = sendAndGetReply(outgoing);

        return reply;
    }

    /**
     * Initiate a map/reduce job.
     *
     * @param sourceNamespace Namespace the data is read from. If null, default namespace will be used.
     * @param targetNamespace Namespace to write the results to.
     * @param script Script to execute.
     * @return ID of the job.
     * @throws ClientException if something goes wrong.
     */
    public String mapReduce(String sourceNamespace, String targetNamespace, String script) throws ClientException {
        // TODO would be better to include a host name, but OK for now
        String jobId = String.format("mr%d", System.currentTimeMillis());
        Range sourceKeyRange = new Range(0, 0); // take all for now
        InitiateMRRequest request = new InitiateMRRequest(jobId, sourceKeyRange, sourceNamespace, targetNamespace, script, null);

        CompletableFuture<InitiateMRResponse> futureResponse =
                // TODO should correctly choose master by ID on the ring instead of always using the seed
                communicationModules.get(seedAddress)
                        .send(request)
                        .thenApply(reply -> (InitiateMRResponse) reply.getMRMessage());

        try {
            InitiateMRResponse response = futureResponse.get(3, TimeUnit.SECONDS);
            return response.getId();
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new ClientException("Could not start map/reduce instance.", e);
        }
    }

    /**
     * Retrieve the status for a map/reduce job.
     *
     * @param jobId ID of the job.
     * @return Status.
     * @throws ClientException If something goes wrong.
     */
    public MRStatusMessage getMapReduceStatus(String jobId) throws ClientException {
        MRStatusRequest request = new MRStatusRequest(jobId, MRStatusRequest.Type.MASTER);
        CompletableFuture<MRStatusMessage> futureResponse =
                // TODO should correctly choose master by ID on the ring instead of always using the seed
                communicationModules.get(seedAddress)
                        .send(request)
                        .thenApply(reply -> (MRStatusMessage) reply.getMRMessage());

        try {
            MRStatusMessage response = futureResponse.get(3, TimeUnit.SECONDS);
            return response;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new ClientException("Could get status for map/reduce job.", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public KVMessage delete(String key) throws ClientException {
        ensureConnected();

        KVMessage outgoing = new DefaultKVMessage(key, null, KVMessage.StatusType.DELETE);
        KVMessage reply = sendAndGetReply(outgoing);

        return reply;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isConnected() {
        return running;
    }

    private KVMessage sendAndGetReply(KVMessage msg) throws ClientException {
        if (hashRing.isEmpty()) {
            throw new ClientException("No more known server nodes to try.");
        }

        try {
            CompletableFuture<KVMessage> futureReply = communicationModuleForKey(msg.getKey())
                    .send(msg)
                    .thenApply(CorrelatedMessage::getKVMessage);

            KVMessage reply = futureReply.get(3, TimeUnit.SECONDS);

            // check if the cluster has been updated possibly retry
            return checkReplyForResponsibility(reply, msg)
                    .get(3, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            if (communicationModuleForKey(msg.getKey()).isRunning()) {
                // request could not be completed for some non-systematic reason
                throw new ClientException("Could not process message.", e);
            } else {
                // server has been stopped
                // redirect the request to the successor node as heuristic
                HostAndPort staleNode = hashRing.getResponsibleNode(msg.getKey());
                HostAndPort successor = hashRing.getSuccessor(staleNode);
                LOG.info("Detected stale node {}. Retrying on successor {}.", staleNode, successor);
                removeNodeConnection(staleNode);
                return sendAndGetReply(msg);
            }
        }
    }

    private CompletableFuture<KVMessage> checkReplyForResponsibility(KVMessage response, KVMessage originalRequest) {
        if (response.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
            // update the local server list
            List<HostAndPort> updatedServers = Stream
                    .of(response.getValue())
                    .map(NodeEntry::mutlipleFromSerializedString)
                    .flatMap(List::stream)
                    .map(node -> node.address)
                    .collect(Collectors.toList());
            LOG.debug("Updating servers from NOT_RESPONSIBLE response: " + updatedServers);
            updateConnections(updatedServers);
            LOG.info("Updated servers from NOT_RESPONSIBLE response: " + updatedServers);

            // retry after updating the server list
            // TODO: can we simply assume that it works the second time?
            return communicationModuleForKey(originalRequest.getKey())
                    .send(originalRequest)
                    .thenApply(CorrelatedMessage::getKVMessage);
        } else {
            return CompletableFuture.completedFuture(response);
        }
    }

    private CommunicationModule communicationModuleForKey(String key) {
        HostAndPort responsibleNode = hashRing.getResponsibleNode(key);
        CommunicationModule communicationModule = communicationModules.get(responsibleNode);
        return communicationModule;
    }

    private synchronized void updateConnections(Collection<HostAndPort> newNodes) {
        Set<HostAndPort> nodesToAdd = new HashSet<>(newNodes);
        nodesToAdd.removeAll(hashRing.getNodes());

        Set<HostAndPort> nodesToRemove = new HashSet<>(hashRing.getNodes());
        nodesToRemove.removeAll(newNodes);

        for (HostAndPort node : nodesToAdd) {
            try {
                addNodeConnection(node);
            } catch (ClientException e) {
                LOG.error("Could not add new connection to node " + node, e);
            }
        }

        for (HostAndPort node : nodesToRemove) {
            removeNodeConnection(node);
        }

        for (HostAndPort node : hashRing.getNodes()) {
            LOG.info("Updated node responsibility {}: {}", node, hashRing.getAssignedRange(node));
        }
    }

    private void addNodeConnection(HostAndPort node) throws ClientException {
        CommunicationModule communicationModule = new CommunicationModule(node);
        communicationModule.start();
        communicationModules.put(node, communicationModule);
        hashRing.addNode(node);
    }

    private void removeNodeConnection(HostAndPort node) {
        hashRing.removeNode(node);
        CommunicationModule communicationModule = communicationModules.remove(node);
        communicationModule.stop();
    }

    private void ensureConnected() throws DisconnectedException {
        if (!isConnected()) {
            throw new DisconnectedException();
        }
    }

}
