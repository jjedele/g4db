package app_kvServer;

import app_kvServer.gossip.Gossiper;
import app_kvServer.persistence.PersistenceException;
import app_kvServer.persistence.PersistenceService;
import common.hash.HashRing;
import common.hash.Range;
import common.messages.gossip.ClusterDigest;
import common.utils.ContextPreservingThread;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Deletes data this node is not responsible for anymore if the cluster has been
 * stable for a given amount of time.
 */
public class CleanUpWorker extends ContextPreservingThread {

    private final Logger LOG = LogManager.getLogger(CleanUpWorker.class);

    private final InetSocketAddress myself;
    private final PersistenceService persistenceService;
    private final int stableTimeSeconds;
    private final int intervalSeconds;
    private final int replicationFactor;
    private volatile long lastClusterChange;
    private long lastCleanUp;

    /**
     * Constructor.
     * @param myself Address of this node
     * @param persistenceService Persistence service
     * @param stableTimeSeconds Time for which the cluster must not change before cleanup starts
     * @param intervalSeconds Time between consecutive cleanup intervals
     * @param replicationFactor Number of nodes data is replicated to
     */
    public CleanUpWorker(InetSocketAddress myself,
                         PersistenceService persistenceService,
                         int stableTimeSeconds,
                         int intervalSeconds,
                         int replicationFactor) {
        this.myself = myself;
        this.persistenceService = persistenceService;
        this.stableTimeSeconds = stableTimeSeconds;
        this.intervalSeconds = intervalSeconds;
        this.replicationFactor = replicationFactor;
        this.lastClusterChange = System.currentTimeMillis();
        this.lastCleanUp = System.currentTimeMillis();
    }

    @Override
    public void run() {
        setUpThreadContext();

        LOG.info("Clean up worker started.");

        long currentTime;
        boolean clusterStable;
        boolean cleanUpDue;

        while (!Thread.currentThread().isInterrupted()) {
            currentTime = System.currentTimeMillis();
            clusterStable = (currentTime - lastClusterChange) > stableTimeSeconds * 1000;
            cleanUpDue = (currentTime - lastCleanUp) > intervalSeconds * 1000;

            if (clusterStable && cleanUpDue) {
                LOG.info("Starting clean up");
                CompletableFuture.runAsync(new CleanUpDataTask());
                lastCleanUp = System.currentTimeMillis();
            } else {
                LOG.info("Skipping clean up. Stable={}, due={}", clusterStable, cleanUpDue);
            }

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }

        LOG.info("Shutting down clean up thread.");
    }

    public void reportChange() {
        this.lastClusterChange = System.currentTimeMillis();
    }

    private class CleanUpDataTask extends ContextPreservingThread {
        @Override
        public void run() {
            setUpThreadContext();

            ClusterDigest clusterDigest = Gossiper.getInstance().getClusterDigest();
            Set<InetSocketAddress> clusterNodes = clusterDigest.getCluster().entrySet().stream()
                    .filter(e -> e.getValue().getStatus().isParticipating())
                    .map(e -> e.getKey())
                    .collect(Collectors.toSet());

            HashRing ring = new HashRing(clusterNodes);

            Set<Range> ownedKeyRanges = IntStream
                    .range(0, replicationFactor)
                    .mapToObj(predecessorNr -> ring.getPredecessor(myself, predecessorNr))
                    .map(ring::getAssignedRange)
                    .collect(Collectors.toSet());

            Predicate<String> keyInResponsibility = (String key) -> ownedKeyRanges.stream()
                    .map(range -> range.contains(HashRing.hash(key)))
                    .anyMatch(contained -> contained);

            try {
                Set<String> keysToBeDeleted = persistenceService.getKeys().stream()
                        .filter(keyInResponsibility.negate())
                        .collect(Collectors.toSet());

                keysToBeDeleted.stream().forEach(key -> {
                    try {
                        persistenceService.delete(key);
                    } catch (PersistenceException e) {
                        LOG.error("Could not clean up key=" + key, e);
                    }
                });

                LOG.info("{} keys have been cleaned up", keysToBeDeleted.size());
            } catch (PersistenceException e) {
                LOG.error("Error cleaning up data.", e);
            }
        }
    }

}
