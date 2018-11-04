package app_kvServer.persistence;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class FIFOCache<K, V> implements Cache<K, V> {
    private final Map<K, V> cacheMap;

    public FIFOCache(int cacheSize) {
        this.cacheMap = new LinkedHashMap<>(cacheSize, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > cacheSize;
            }
        };
    }

    @Override
    public V get(K key) throws PersistenceException {
        if (cacheMap.get(key) == null) {
            throw new NoSuchElementException("No such element: " + cacheMap.get(key));
        }
        return cacheMap.get(key);
    }

    @Override
    public void put(K key, V value) throws PersistenceException {
        cacheMap.put(key, value);
    }

    @Override
    public boolean contains(K key) throws PersistenceException {
        return cacheMap.containsKey(key);
    }

    @Override
    public void delete(K key) throws PersistenceException {
        if (cacheMap.get(key) == null) {
            throw new NoSuchElementException("No such element: " + cacheMap.get(key));
        }
        cacheMap.remove(key);
    }
}
