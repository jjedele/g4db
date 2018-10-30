package app_kvServer.persistence;

public interface Cache<K, V> {

    V get(K key) throws PersistenceException;

    void put(K key, V value) throws PersistenceException;

    boolean contains(K key) throws PersistenceException;

    void delete(K key) throws PersistenceException;

}
