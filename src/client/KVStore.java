package client;

import client.exceptions.*;
import common.CorrelatedMessage;
import common.hash.HashRing;
import common.hash.NodeEntry;
import common.messages.DefaultKVMessage;
import common.messages.KVMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KVStore implements KVCommInterface {

    private static final Logger LOG = LogManager.getLogger(KVStore.class);

    private final HashRing hashRing;
    private final Map<InetSocketAddress, CommunicationModule> communicationModules;
    private boolean running;
    private final InetSocketAddress seedAddress;

    /**
     * Initialize KVStore with address and port of KVServer
     *
     * @param address the address of the seed KVServer
     * @param port    the port of the seed KVServer
     */
    public KVStore(String address, int port) {
        this.hashRing = new HashRing();
        this.communicationModules = new ConcurrentHashMap<>();
        this.seedAddress = new InetSocketAddress(address, port);
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
                InetSocketAddress staleNode = hashRing.getResponsibleNode(msg.getKey());
                InetSocketAddress successor = hashRing.getSuccessor(staleNode);
                LOG.info("Detected stale node {}. Retrying on successor {}.", staleNode, successor);
                removeNodeConnection(staleNode);
                return sendAndGetReply(msg);
            }
        }
    }

    private CompletableFuture<KVMessage> checkReplyForResponsibility(KVMessage response, KVMessage originalRequest) {
        if (response.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
            // update the local server list
            List<InetSocketAddress> updatedServers = Stream
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
        InetSocketAddress responsibleNode = hashRing.getResponsibleNode(key);
        CommunicationModule communicationModule = communicationModules.get(responsibleNode);
        return communicationModule;
    }

    private synchronized void updateConnections(Collection<InetSocketAddress> newNodes) {
        Set<InetSocketAddress> nodesToAdd = new HashSet<>(newNodes);
        nodesToAdd.removeAll(hashRing.getNodes());

        Set<InetSocketAddress> nodesToRemove = new HashSet<>(hashRing.getNodes());
        nodesToRemove.removeAll(newNodes);

        for (InetSocketAddress node : nodesToAdd) {
            try {
                addNodeConnection(node);
            } catch (ClientException e) {
                LOG.error("Could not add new connection to node " + node, e);
            }
        }

        for (InetSocketAddress node : nodesToRemove) {
            removeNodeConnection(node);
        }

        for (InetSocketAddress node : hashRing.getNodes()) {
            LOG.info("Updated node responsibility {}: {}", node, hashRing.getAssignedRange(node));
        }
    }

    private void addNodeConnection(InetSocketAddress node) throws ClientException {
        CommunicationModule communicationModule = new CommunicationModule(node, 1000);
        communicationModule.start();
        communicationModules.put(node, communicationModule);
        hashRing.addNode(node);
    }

    private void removeNodeConnection(InetSocketAddress node) {
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
