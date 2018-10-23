package client;

import java.io.IOException;

/**
 * Interface for the database client.
 * @deprecated Will be replaces by {@link KVCommInterface}
 */
@Deprecated()
public interface Client {

    /**
     * Connect the client to server.
     * @param hostname the name of the host
     * @param port the port to connect to
     * @return welcome message of the server
     * @throws IOException if something network related goes wrong
     */
    String connect(String hostname, int port) throws IOException;

    /**
     * Disconnect from the the server
     * @throws IOException if something network related goes wrong
     */
    void disconnect() throws IOException;

    /**
     * Send a message to the server and return the reply.
     * @param message the message to send
     * @return the reply
     * @throws IOException if something network related goes wrong
     */
    String sendMessage(String message) throws IOException;

    /**
     * Return if the client is currently connected.
     * @return connection status
     */
    boolean isConnected();

}
