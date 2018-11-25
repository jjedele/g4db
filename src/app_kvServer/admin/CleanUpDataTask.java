package app_kvServer.admin;

import app_kvServer.persistence.PersistenceException;
import app_kvServer.persistence.PersistenceService;
import common.hash.HashRing;
import common.hash.Range;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Task that deletes entries for keys the server lost responsibility for after a cluster change.
 */
public class CleanUpDataTask implements AdminTask {

    private static final Logger LOG = LogManager.getLogger(CleanUpDataTask.class);

    private final PersistenceService persistenceService;
    private final Range keyRange;
    private final AtomicInteger counter;
    private List<String> keysToDelete;

    /**
     * Constructor.
     * @param persistenceService Persistence service to delete keys from
     * @param keyRange Range of keys to keep
     */
    public CleanUpDataTask(PersistenceService persistenceService, Range keyRange) {
        this.persistenceService = persistenceService;
        this.keyRange = keyRange;
        this.counter = new AtomicInteger(0);
    }

    @Override
    public float getProgress() {
        if (keysToDelete != null && keysToDelete.size() == 0) {
            // no work
            return 1;
        } else {
            int divider = keysToDelete != null ? keysToDelete.size() : 1;
            return (float) counter.get() / divider;
        }
    }

    @Override
    public void run() {
        // assemble a list of keys we want to transfer
        try {
            keysToDelete = persistenceService.getKeys().stream()
                    .filter(key -> !keyRange.contains(HashRing.hash(key)))
                    .collect(Collectors.toList());

            LOG.info("Starting clean up of {} keys", keysToDelete.size());
            keysToDelete.stream()
                    .forEach(key -> {
                        try {
                            persistenceService.delete(key);
                        } catch (PersistenceException e) {
                            LOG.error("Could not delete key: " + key, e);
                        } finally {
                            this.counter.incrementAndGet();
                        }
                    });

            LOG.info("Finished data clean up.");
        } catch (PersistenceException e) {
            LOG.error("Could not delete key range: " + keyRange, e);
        }
    }

}
