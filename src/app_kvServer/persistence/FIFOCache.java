package app_kvServer.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * https://effective-java.com/2011/05/simple-lru-or-fifo-cache/
 *
 * @param <K> Type of the keys
 * @param <V> Type of the values
 */
public class FIFOCache<K, V> implements Cache<K, V> {
    private static final Logger LOG = LogManager.getLogger(FIFOCache.class);
    private final Map<K, V> cacheMap;

    /**
     * Default constructor
     *
     * @param cacheSize Number of elements to hold
     */
    public FIFOCache(int cacheSize) {
        this.cacheMap = new LinkedHashMap<K, V>(cacheSize, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                LOG.debug("Ejected element: {}", eldest);
                return size() > cacheSize;
            }
        };
    }

    /**
     * {@inheritDoc}
     *
     * @param key
     * @return value of the key
     */
    @Override
    public Optional<V> get(K key) {
        return Optional.ofNullable(cacheMap.get(key));
    }

    /**
     * {@inheritDoc}
     *
     * @param key
     * @param value
     */
    @Override
    public void put(K key, V value) {
        cacheMap.put(key, value);
    }

    /**
     * {@inheritDoc}
     *
     * @param key
     * @return boolean if the key is contained
     */
    @Override
    public boolean contains(K key) {
        return cacheMap.containsKey(key);
    }

    /**
     * {@inheritDoc}
     *
     * @param key
     */
    @Override
    public boolean delete(K key) {
        if (cacheMap.get(key) == null) {
            return false;
        }
        cacheMap.remove(key);
        return true;
    }
}
