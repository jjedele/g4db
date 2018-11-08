package client;

import client.exceptions.ClientException;
import common.hash.NodeEntry;
import common.hash.Range;
import common.messages.admin.GenericResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.Collection;

/**
 * Dummy implementation of the admin client.
 */
@Deprecated
public class DummyAdminClient implements KVAdminInterface {

    private final Logger LOG = LogManager.getLogger(DummyAdminClient.class);

    private final InetSocketAddress serverAddress;

    public DummyAdminClient(InetSocketAddress serverAddress) {
        this.serverAddress = serverAddress;
    }

    @Override
    public GenericResponse updateMetadata(Collection<NodeEntry> nodes) throws ClientException {
        LOG.info("Server {}: Update metadata: {}", serverAddress, nodes);
        return GenericResponse.success("Not implemented.");
    }

    @Override
    public GenericResponse start() throws ClientException {
        LOG.info("Server {}: Start", serverAddress);
        return GenericResponse.success("Not implemented.");
    }

    @Override
    public GenericResponse stop() throws ClientException {
        LOG.info("Server {}: Stop", serverAddress);
        return GenericResponse.success("Not implemented.");
    }

    @Override
    public GenericResponse shutDown() throws ClientException {
        LOG.info("Server {}: Shut down", serverAddress);
        return GenericResponse.success("Not implemented.");
    }

    @Override
    public GenericResponse enableWriteLock() throws ClientException {
        LOG.info("Server {}: Enable write lock", serverAddress);
        return GenericResponse.success("Not implemented.");
    }

    @Override
    public GenericResponse disableWriteLock() throws ClientException {
        LOG.info("Server {}: Disable write lock", serverAddress);
        return GenericResponse.success("Not implemented.");
    }

    @Override
    public GenericResponse moveData(InetSocketAddress destination, Range keyRange) throws ClientException {
        LOG.info("Server {}: Move data to {}, range {}", serverAddress, destination, keyRange);
        return GenericResponse.success("Not implemented.");
    }

    @Override
    public void connect() throws ClientException {
        LOG.info("Server {}: Connect", serverAddress);
    }

    @Override
    public void disconnect() {
        LOG.info("Server {}: Disconnect", serverAddress);
    }

    @Override
    public boolean isConnected() {
        return false;
    }
}
