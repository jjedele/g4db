package client;

import client.exceptions.ClientException;
import client.exceptions.DisconnectedException;
import common.CorrelatedMessage;
import common.hash.NodeEntry;
import common.hash.Range;
import common.messages.admin.*;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Default implementation of a KV admin client.
 */
public class KVAdmin implements KVAdminInterface {

    private final InetSocketAddress address;
    private final CommunicationModule communicationModule;
    private int timeoutSeconds;

    /**
     * Constructor.
     * @param server Address of the server to connect to.
     */
    public KVAdmin(InetSocketAddress server) {
        this.address = server;
        this.communicationModule = new CommunicationModule(server, 1000);
        this.timeoutSeconds = 30;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetSocketAddress getNodeAddress() {
        return address;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenericResponse updateMetadata(Collection<NodeEntry> nodes) throws ClientException {
        ensureConnected();

        UpdateMetadataRequest request = new UpdateMetadataRequest();
        for (NodeEntry nodeEntry : nodes) {
            // TODO change API to inet addresses only after merging
            request.addNode(nodeEntry.address);
        }

        return executeGenericReplySynchronously(request);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenericResponse start() throws ClientException {
        ensureConnected();

        return executeGenericReplySynchronously(new StartServerRequest());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenericResponse stop() throws ClientException {
        ensureConnected();

        return executeGenericReplySynchronously(new StopServerRequest());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenericResponse shutDown() throws ClientException {
        ensureConnected();

        return executeGenericReplySynchronously(new ShutDownServerRequest());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenericResponse enableWriteLock() throws ClientException {
        ensureConnected();

        return executeGenericReplySynchronously(new EnableWriteLockRequest());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenericResponse disableWriteLock() throws ClientException {
        ensureConnected();

        return executeGenericReplySynchronously(new DisableWriteLockRequest());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenericResponse moveData(InetSocketAddress destination, Range keyRange) throws ClientException {
        MoveDataRequest moveDataRequest = new MoveDataRequest(destination, keyRange);

        return executeGenericReplySynchronously(moveDataRequest);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void connect() throws ClientException {
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
    public boolean isConnected() {
        return communicationModule.isRunning();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MaintenanceStatusResponse getMaintenanceStatus() throws ClientException {
        try {
            return (MaintenanceStatusResponse) communicationModule
                    .send(new GetMaintenanceStatusRequest())
                    .thenApply(CorrelatedMessage::getAdminMessage)
                    .get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new ClientException("Could not execute request.", e);
        }
    }

    private void ensureConnected() throws ClientException {
        if (!communicationModule.isRunning()) {
            throw new DisconnectedException();
        }
    }

    private GenericResponse executeGenericReplySynchronously(AdminMessage msg) throws ClientException {
        try {
            return (GenericResponse) communicationModule
                    .send(msg)
                    .thenApply(CorrelatedMessage::getAdminMessage)
                    .get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new ClientException("Could not execute request.", e);
        }
    }

}
