package app_kvServer.persistence;

import javax.swing.text.html.Option;
import java.util.List;
import java.util.Optional;

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
     * @return an optional containing the value or being empty if the value does not exist
     * @throws PersistenceException if something goes wrong while getting the value
     */
    Optional<String> get(String key) throws PersistenceException;

    /**
     * Delete a key from persistent storage.
     * @param key the key to delete
     * @return True if a value was deleted
     * @throws PersistenceException if something goes wrong
     */
    boolean delete(String key) throws PersistenceException;

    /**
     * Check if a key is persisted.
     * @param key the key
     * @return true if a value for the key exists
     * @throws PersistenceException if something goes wrong
     */
    boolean contains(String key) throws PersistenceException;

    /**
     * Get a list of all persisted keys.
     * @return List of persisted keys
     * @throws PersistenceException if something goes wrong
     */
    List<String> getKeys() throws PersistenceException;

    /**
     * Get a list of all persisted keys in a given namespace.
     * @param namespace Namespace to list keys for
     * @return List of persisted keys
     * @throws PersistenceException if something goes wrong
     */
    List<String> getKeys(String namespace) throws PersistenceException;

}
