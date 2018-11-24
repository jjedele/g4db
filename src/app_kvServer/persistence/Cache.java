package app_kvServer.persistence;

import java.util.Optional;

public interface Cache<K, V> {

    Optional<V> get(K key) throws PersistenceException;

    void put(K key, V value) throws PersistenceException;

    boolean contains(K key) throws PersistenceException;

    boolean delete(K key) throws PersistenceException;

}
