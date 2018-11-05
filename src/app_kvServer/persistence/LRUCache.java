package app_kvServer.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * https://effective-java.com/2011/05/simple-lru-or-fifo-cache/
 *
 * @param <K> Type of the keys
 * @param <V> Type of the values
 */

public class LRUCache<K,V> implements Cache<K,V>  {
    private static final Logger LOG = LogManager.getLogger(LRUCache.class);

    private final Map<K, V> cacheMap;

    /**
     * Default constructor
     * @param cacheSize Number of elements to hold
     */
    public LRUCache(int cacheSize) {
        this.cacheMap = new LinkedHashMap<>(cacheSize, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                LOG.debug("Ejected element: {}", eldest);
                return size() > cacheSize;
            }
        };
    }

    /**
     * {@inheritDoc}
     * @param key
     * @return key with its value
     */
    @Override
    public V get(K key) {
        if (cacheMap.get(key) == null ) {
            throw new NoSuchElementException("No such element: " + cacheMap.get(key));
        }
        return cacheMap.get(key);
    }

    /**
     * {@inheritDoc}
     * @param key
     * @param value
     */
    @Override
    public  void put(K key, V value) {
        cacheMap.put(key, value);
    }

    /**
     * {@inheritDoc}
     * @param key
     * @return boolean if the key is contained
     */
    @Override
    public boolean contains(K key) {
        return cacheMap.containsKey(key);
    }

    /**
     * {@inheritDoc}
     * @param key
     */
    @Override
    public void delete(K key) {
        if (cacheMap.get(key) == null) {
            throw new NoSuchElementException("No such element: " + cacheMap.get(key));
        }
        cacheMap.remove(key);
    }

}
