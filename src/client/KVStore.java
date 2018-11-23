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

    /**
     * Initialize KVStore with address and port of KVServer
     *
     * @param address the address of the seed KVServer
     * @param port    the port of the seed KVServer
     */
    public KVStore(String address, int port) {
        this.hashRing = new HashRing();
        this.communicationModules = new ConcurrentHashMap<>();
        // TODO this immediately starts the module right now, see if this is a problem
        addNodeConnection(new InetSocketAddress(address, port));
        this.running = true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void connect() {
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
        try {
            return communicationModuleForKey(msg.getKey())
                    .send(msg)
                    .thenApply(CorrelatedMessage::getKVMessage)
                    .thenCompose(reply -> checkReplyForResponsibility(reply, msg))
                    .get(30, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new ClientException("Could not process message.", e);
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
            addNodeConnection(node);
        }

        for (InetSocketAddress node : nodesToRemove) {
            removeNodeConnection(node);
        }
    }

    private void addNodeConnection(InetSocketAddress node) {
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
