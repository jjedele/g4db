package client;

import common.messages.KVMessage;

import java.io.IOException;

public interface KVCommInterface {

    /**
     * Establishes a connection to the KV Server.
     *
     * @throws IOException if connection could not be established.
     */
    public void connect() throws IOException;

    /**
     * disconnects the client from the currently connected server.
     */
    public void disconnect();

    /**
     * Inserts a key-value pair into the KVServer.
     *
     * @param key   the key that identifies the given value.
     * @param value the value that is indexed by the given key.
     * @return a message that confirms the insertion of the tuple or an error.
     * @throws IOException if put command cannot be executed (e.g. not connected to any
     *                   KV server).
     */
    public KVMessage put(String key, String value) throws IOException;

    /**
     * Retrieves the value for a given key from the KVServer.
     *
     * @param key the key that identifies the value.
     * @return the value, which is indexed by the given key.
     * @throws IOException if put command cannot be executed (e.g. not connected to any
     *                   KV server).
     */
    public KVMessage get(String key) throws IOException;
}
