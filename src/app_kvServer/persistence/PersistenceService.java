package app_kvServer.persistence;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Base interface for the persistence service.
 */
public interface PersistenceService {

    /**
     * Take a key,value-pair and put it to disk.
     * @param key the key
     * @param value the value
     * @return true if the key was inserted, false if it was updated
     * @throws PersistenceException if something goes wrong while persisting
     */

    boolean put(String key, String value) throws PersistenceException;
    /**
     * Retrieve the value associated with key.
     * @param key the key to retrieve the value for
     * @return the value
     * @throws PersistenceException if something goes wrong while getting the value
     */
    String get(String key) throws PersistenceException, IOException;

    /**
     * Delete a key from persistent storage.
     * @param key the key to delete
     * @throws PersistenceException if something goes wrong
     */
    void delete(String key) throws PersistenceException;

    /**
     * Check if a key is persisted.
     * @param key the key
     * @return true if a value for the key exists
     * @throws PersistenceException if something goes wrong
     */
    boolean contains(String key) throws PersistenceException;

}
