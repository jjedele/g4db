package app_kvServer.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class LRUCache<K,V> implements Cache<K,V>  {
    private final Map<K, V> cacheMap;

    public LRUCache(int cacheSize) {
        this.cacheMap = new LinkedHashMap<>(cacheSize, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > cacheSize;
            }
        };
    }

    @Override
    public V get(K key) {
        if (cacheMap.get(key) == null ) {
            throw new NoSuchElementException("No such element: " + cacheMap.get(key));
        }
        return cacheMap.get(key);
    }

    @Override
    public  void put(K key, V value) {
        cacheMap.put(key, value);
    }

    @Override
    public boolean contains(K key) {
        return cacheMap.containsKey(key);
    }

    @Override
    public void delete(K key) {
        if (cacheMap.get(key) == null) {
            throw new NoSuchElementException("No such element: " + cacheMap.get(key));
        }
        cacheMap.remove(key);
    }

}
