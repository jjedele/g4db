package client;

import client.exceptions.ClientException;

/**
 * Base interface for a connection to the server.
 */
public interface KVInterface {

    /**
     * Connect to the server.
     * @throws ClientException if connection can not be established
     */
    void connect() throws ClientException;

    /**
     * Disconnect from the server.
     */
    void disconnect();

    /**
     * Check if a connection to the server is established.
     * @return true if connected
     */
    boolean isConnected();


}
