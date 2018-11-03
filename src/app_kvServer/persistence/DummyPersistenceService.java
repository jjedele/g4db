package app_kvServer.persistence;

import app_kvServer.CacheReplacementStrategy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Dummy persistence service.
 * TODO remove
 */
@Deprecated
public class DummyPersistenceService implements PersistenceService {

    private final static Logger LOG = LogManager.getLogger(DummyPersistenceService.class);

    private final Map<String, String> map;

    public DummyPersistenceService(int cacheSize, CacheReplacementStrategy displacementStrategy) {
        LOG.info("Dummy persistence service created with size {} {} cache",
                cacheSize,
                displacementStrategy);
        this.map = new HashMap<>();
    }

    @Override
    public boolean put(String key, String value) throws PersistenceException {
        LOG.info("Persist called: {} -> {}", key, value);
        boolean insert = !map.containsKey(key);
        map.put(key, value);
        return insert;
    }

    @Override
    public String get(String key) throws PersistenceException {
        LOG.info("Get called: {}", key);
        if (map.containsKey(key)) {
            return map.get(key);
        } else {
            throw new PersistenceException(String.format("Key %s not found.", key));
        }
    }

    @Override
    public void delete(String key) throws PersistenceException {
        LOG.info("Delete called: {}", key);
        if (map.containsKey(key)) {
            map.remove(key);
        } else {
            throw new PersistenceException(String.format("Key %s not found.", key));
        }
    }

    @Override
    public boolean contains(String key) throws PersistenceException {
        LOG.info("Contains called: {}", key);
        return map.containsKey(key);
    }


}
