package app_kvServer.admin;

import app_kvServer.persistence.PersistenceException;
import app_kvServer.persistence.PersistenceService;
import common.hash.HashRing;
import common.hash.Range;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;
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
    private int total;

    /**
     * Constructor.
     * @param persistenceService Persistence service to delete keys from
     * @param keyRange Range of keys to keep
     */
    public CleanUpDataTask(PersistenceService persistenceService, Range keyRange) {
        this.persistenceService = persistenceService;
        this.keyRange = keyRange;
        this.counter = new AtomicInteger(0);
        this.total = 1;
    }

    @Override
    public float getProgress() {
        return (float) counter.get() / total;
    }

    @Override
    public void run() {
        // assemble a list of keys we want to transfer
        try {
            List<String> keysToTransfer = persistenceService.getKeys().stream()
                    .filter(key -> !keyRange.contains(HashRing.hash(key)))
                    .collect(Collectors.toList());
            this.total = keysToTransfer.size();

            LOG.info("Starting clean up of {} keys", total);
            keysToTransfer.stream()
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
