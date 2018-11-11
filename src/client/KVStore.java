package client;

import client.exceptions.*;
import common.CorrelatedMessage;
import common.messages.DefaultKVMessage;
import common.messages.KVMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KVStore implements KVCommInterface {

    private static final Logger LOG = LogManager.getLogger(KVStore.class);

    private final CommunicationModule communicationModule;

    /**
     * Initialize KVStore with address and port of KVServer
     *
     * @param address the address of the KVServer
     * @param port    the port of the KVServer
     */
    public KVStore(String address, int port) {
        this.communicationModule = new CommunicationModule(
                new InetSocketAddress(address, port), 1000);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void connect() {
        communicationModule.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void disconnect() {
        communicationModule.stop();
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
        return communicationModule.isRunning();
    }

    private synchronized KVMessage sendAndGetReply(KVMessage msg) throws ClientException {
        try {
            return communicationModule
                    .send(msg)
                    .thenApply(CorrelatedMessage::getKVMessage)
                    .get(30, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new ClientException("Could not process message.", e);
        }
    }

    private void ensureConnected() throws DisconnectedException {
        if (!isConnected()) {
            throw new DisconnectedException();
        }
    }

}
