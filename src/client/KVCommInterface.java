package client;

import client.exceptions.ClientException;
import common.messages.KVMessage;

import java.io.IOException;

public interface KVCommInterface extends KVInterface {

    /**
     * Inserts a key-value pair into the KVServer.
     *
     * @param key   the key that identifies the given value.
     * @param value the value that is indexed by the given key.
     * @return a message that confirms the insertion of the tuple or an error.
     * @throws IOException if put command cannot be executed (e.g. not connected to any
     *                   KV server).
     */
    KVMessage put(String key, String value) throws ClientException;

    /**
     * Retrieves the value for a given key from the KVServer.
     *
     * @param key the key that identifies the value.
     * @return the value, which is indexed by the given key.
     * @throws IOException if put command cannot be executed (e.g. not connected to any
     *                   KV server).
     */
    KVMessage get(String key) throws ClientException;

}
