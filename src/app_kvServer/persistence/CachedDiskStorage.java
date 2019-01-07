package app_kvServer.persistence;

import app_kvServer.CacheReplacementStrategy;

import java.io.File;
import java.util.List;
import java.util.Optional;

/**
 * Persists values directly to disk and maintains a configurable
 * cache for faster retrievals.
 */
public class CachedDiskStorage implements PersistenceService {

    private final Cache<String, String> cache;
    private final PersistenceService diskStorage;

    // TODO: show if synchronization can be done in a smarter way

    /**
     * Default constructor.
     * @param dataDirectory The directory where data files are stored
     * @param cacheSize Number of elements the cache can hold
     * @param replacementStrategy Displacement strategy for the cache
     */
    public CachedDiskStorage(File dataDirectory, int cacheSize, CacheReplacementStrategy replacementStrategy) {
        this.diskStorage = new DiskStorage(dataDirectory);
        switch (replacementStrategy) {
            case LFU:
                cache = new LFUCache<>(cacheSize);
                break;
            case LRU:
                cache = new LRUCache<>(cacheSize);
                break;
            case FIFO:
            default:
                cache = new FIFOCache<>(cacheSize);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized boolean put(String key, String value) throws PersistenceException {
        boolean insert = !cache.contains(key) && !diskStorage.contains(key);

        diskStorage.put(key, value);
        cache.put(key, value);

        return insert;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Optional<String> get(String key) throws PersistenceException {
        Optional<String> cacheResult = cache.get(key);
        if (cacheResult.isPresent()) {
            return cacheResult;
        } else {
            return diskStorage.get(key);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized boolean delete(String key) throws PersistenceException {
        boolean deleted = diskStorage.delete(key);
        if (cache.contains(key)) {
            cache.delete(key);
        }
        return deleted;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized boolean contains(String key) throws PersistenceException {
        return cache.contains(key) || diskStorage.contains(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getKeys() throws PersistenceException {
        return diskStorage.getKeys();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getKeys(String namespace) throws PersistenceException {
        return diskStorage.getKeys(namespace);
    }

}
