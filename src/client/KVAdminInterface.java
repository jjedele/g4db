package client;

import client.exceptions.ClientException;
import common.hash.NodeEntry;
import common.hash.Range;
import common.messages.admin.GenericResponse;
import common.messages.admin.MaintenanceStatusResponse;

import java.net.InetSocketAddress;
import java.util.Collection;

/**
 * Interface to the administration capabilities of the KV server.
 */
public interface KVAdminInterface extends KVInterface {

    /**
     * Request the server to update it's metadata table.
     * @param nodes A list of node responsibility entries
     * @return Server response
     * @throws ClientException If something goes wrong
     */
    GenericResponse updateMetadata(Collection<NodeEntry> nodes) throws ClientException;

    /**
     * Start the server.
     *
     * Metadata must be updated before this can happen.
     *
     * @return Server response
     * @throws ClientException If something goes wrong
     */
    GenericResponse start() throws ClientException;

    /**
     * Stop the server.
     * @return Server response
     * @throws ClientException If something goes wrong
     */
    GenericResponse stop() throws ClientException;

    /**
     * Shutdown the server completely.
     * @return Server response
     * @throws ClientException If something goes wrong
     */
    GenericResponse shutDown() throws ClientException;

    /**
     * Request the server to not accept writes anymore.
     *
     * This should be done before the keys are transferred to another
     * server.
     *
     * @return Server response
     * @throws ClientException If something goes wrong
     */
    GenericResponse enableWriteLock() throws ClientException;

    /**
     * Request the server to accept writes again.
     *
     * This should happen after keys have been transferred successfully.
     *
     * @return Server response
     * @throws ClientException If something goes wrong
     */
    GenericResponse disableWriteLock() throws ClientException;

    /**
     * Requests the server to move a specified range of keys to the given
     * destination server.
     *
     * @param destination
     * @param keyRange
     * @return
     * @throws ClientException
     */
    GenericResponse moveData(InetSocketAddress destination, Range keyRange) throws ClientException;

    /**
     * Requests the server for the status of possible running maintenance tasks.
     *
     * @return Status
     * @throws ClientException if something goes wrong
     */
    MaintenanceStatusResponse getMaintenanceStatus() throws ClientException;

}
