package app_kvServer.persistence;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Base interface for the persistence service.
 */
public interface PersistenceService {

    /**
     * Take a key,value-pair and persist it to disk.
     * @param key the key
     * @param value the value
     * @throws PersistenceException if something goes wrong while persisting
     */
    void persist(String key, String value) throws PersistenceException, IOException;

    /**
     * Retrieve the value associated with key.
     * @param key the key to retrieve the value for
     * @return the value
     * @throws PersistenceException if something goes wrong while getting the value
     */
    String get(String key) throws PersistenceException, IOException;

}
