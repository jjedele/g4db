package app_kvServer.persistence;

import app_kvServer.CacheReplacementStrategy;

import java.io.File;
import java.io.IOException;

public class CachedDiskStorage implements PersistenceService {

    private final Cache<String, String> cache;
    private final PersistenceService diskStorage;

    public CachedDiskStorage(File dataDirectory, int cacheSize, CacheReplacementStrategy replacementStrategy) {
        this.diskStorage = new DiskStorage(dataDirectory);
        switch (replacementStrategy) {
            case LFU:
                cache = new LFUCache<>(cacheSize);
                break;
            case LRU:
                // TODO change to LRU
                cache = new LFUCache<>(cacheSize);
                break;
            case FIFO:
                // TODO change to FIFO
                cache = new LFUCache<>(cacheSize);
                break;
            default:
                // TODO decide on default, but its not really necessary
                cache = new LFUCache<>(cacheSize);
        }
    }

    @Override
    public boolean put(String key, String value) throws PersistenceException {
        diskStorage.put(key, value);
        cache.put(key, value);

        return diskStorage.contains(key) && cache.contains(key);
    }

    @Override
    public String get(String key) throws PersistenceException, IOException {
        if (cache.contains(key)) return cache.get(key);
        return diskStorage.get(key);
    }

    @Override
    public void delete(String key) throws PersistenceException {
        diskStorage.delete(key);
        cache.delete(key);
    }

    @Override
    public boolean contains(String key) throws PersistenceException {
        return cache.contains(key) || diskStorage.contains(key);
    }

}
