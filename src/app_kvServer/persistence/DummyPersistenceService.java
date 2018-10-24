package app_kvServer.persistence;

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

    public DummyPersistenceService(int cacheSize, String displacementStrategy) {
        LOG.info("Dummy persistence service created with size {} {} cache",
                cacheSize,
                displacementStrategy);
        this.map = new HashMap<>();
    }

    @Override
    public void persist(String key, String value) throws PersistenceException {
        LOG.info("Persist called: {} -> {}", key, value);
        map.put(key, value);
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
}
