package app_kvServer.mapreduce;

import app_kvServer.gossip.Gossiper;
import client.CommunicationModule;
import client.KVStore;
import client.exceptions.ClientException;
import common.CorrelatedMessage;
import common.hash.HashRing;
import common.hash.Range;
import common.messages.mapreduce.InitiateMRRequest;
import common.messages.mapreduce.InitiateMRResponse;
import common.messages.mapreduce.MRStatusMessage;
import common.messages.mapreduce.MRStatusRequest;
import common.utils.ContextPreservingThread;
import common.utils.FutureUtils;
import common.utils.HostAndPort;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import javax.script.ScriptException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Map/reduce master process.
 *
 * Initiates and supervises the map/reduce processing in the cluster.
 */
public class MapReduceMaster extends ContextPreservingThread {

    private static final Logger LOG = LogManager.getLogger(MapReduceMaster.class);

    private final HostAndPort myself;
    private final InitiateMRRequest request;
    private final int replicationFactor;

    private HashRing ring;
    private final Map<Range, WorkerSentinel> pendingResults;
    private final Set<Range> finishedResults;
    private final AtomicInteger workerFailures;
    private final MapReduceProcessor resultProcessor;

    private Instant jobStart;

    /**
     * Constructor.
     *
     * @param myself Address of the current node.
     * @param request Request that triggered the map/reduce process.
     * @throws ScriptException If map/reduce script contains errors.
     */
    public MapReduceMaster(HostAndPort myself, InitiateMRRequest request) throws ScriptException {
        this.myself = myself;
        this.request = request;
        this.replicationFactor = 3;

        this.ring = new HashRing(Gossiper.getInstance().getClusterDigest().getCluster().keySet());

        this.pendingResults = new ConcurrentHashMap<>();
        this.finishedResults = new CopyOnWriteArraySet<>();
        this.workerFailures = new AtomicInteger(0);

        this.resultProcessor = new MapReduceProcessor(request.getScript());
    }

    /**
     * Run the map/reduce process.
     */
    @Override
    public void run() {
        setUpThreadContext();
        ThreadContext.put("mrJob", request.getId());
        jobStart = Instant.now();
        LOG.info("Starting map/reduce master process with id={}.", request.getId());

        ring.getNodes().forEach(node -> pendingResults
                .put(ring.getAssignedRange(node),
                        new WorkerSentinel(ring.getAssignedRange(node), node)));

        List<CompletableFuture<Void>> responses = pendingResults.entrySet().stream()
                .map(e -> CompletableFuture.runAsync(e.getValue()))
                .collect(Collectors.toList());

        CompletableFuture<Void> totalFederationFuture = FutureUtils.allOf(responses);
        try {
            totalFederationFuture.get(60, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.error("Could not federate map/reduce request to all nodes.", e);
        }
    }

    /**
     * Collect results from a worker.
     *
     * @param range The key-range covered by the results at hand.
     * @param results Pre-aggregated results.
     */
    public void collectResults(Range range, Map<String, String> results) {
        synchronized (pendingResults) {
            if (pendingResults.containsKey(range)) {
                LOG.info("Received data from completed m/r worker for job={}, range={}",
                        request.getId(), range);

                results.entrySet().stream()
                        .forEach(e -> resultProcessor.processMapResult(e.getKey(), e.getValue()));
                WorkerSentinel node = pendingResults.remove(range);
                node.complete();
                finishedResults.add(node.range);

                if (pendingResults.isEmpty()) {
                    finishJob();
                }
            } else {
                LOG.warn("Duplicate m/r results received for job={}, range={}",
                        request.getId(), request.getSourceKeyRange());
            }
        }
    }

    /**
     * Return the current status of this job.
     *
     * @return Status.
     */
    public MRStatusMessage getStatus() {
        String id = request.getId();
        int workersComplete = finishedResults.size();
        int workersTotal = workersComplete + pendingResults.size();
        int workersFailed = 0; // TODO
        long extentPending = pendingResults.keySet().stream().mapToLong(Range::getExtent).sum();
        long extentComplete = finishedResults.stream().mapToLong(Range::getExtent).sum();
        int percentageComplete = (int) Math.round(100.0 * extentComplete / (extentComplete + extentPending));
        MRStatusMessage.Status status =
                pendingResults.isEmpty() ? MRStatusMessage.Status.FINISHED : MRStatusMessage.Status.RUNNING;
        String error = null;

        return new MRStatusMessage(id, status, workersTotal, workersComplete, workersFailed, percentageComplete, error);
    }

    private void finishJob() {
        Map<String, String> results = resultProcessor.getResults();

        KVStore kvStore = new KVStore(myself.getHost(), myself.getPort());
        try {
            kvStore.connect();

            for (Map.Entry<String, String> e : results.entrySet()) {
                kvStore.put(String.format("%s/%s", request.getTargetNamespace(), e.getKey()), e.getValue());
            }

            Instant jobEnd = Instant.now();
            LOG.info("Finished map/reduce process with id={}, result size={}, duration={}s.",
                    request.getId(), results.entrySet().size(), Duration.between(jobStart, jobEnd).getSeconds());
        } catch (ClientException e) {
            LOG.error("Could not persist m/r results for job=" + request.getId(), e);
        } finally {
            kvStore.disconnect();
        }
    }

    private class WorkerSentinel extends ContextPreservingThread {

        private final Logger log = LogManager.getLogger(WorkerSentinel.class);

        private final Range range;
        private final HostAndPort rangeOwner;
        private final int checkIntervalSeconds = 30;
        private final CompletableFuture<Void> responseFuture;

        public WorkerSentinel(Range range, HostAndPort rangeOwner) {
            this.range = range;
            this.rangeOwner = rangeOwner;
            this.responseFuture = new CompletableFuture<>();
        }

        @Override
        public void run() {
            setUpThreadContext();
            ThreadContext.put("mrRange", range.toString());

            int currentReplica = 0;

            while (!responseFuture.isDone()) {
                HostAndPort target = ring.getNthSuccessor(rangeOwner, currentReplica);

                CompletableFuture<InitiateMRResponse> federationResponse = federateRequest(target, range);
                // TODO check for errors

                while (!responseFuture.isDone()) {
                    try {
                        responseFuture.get(checkIntervalSeconds, TimeUnit.SECONDS);
                        break;
                    } catch (InterruptedException | ExecutionException e) {
                        log.warn("Error while checking response state. Continuing.", e);
                        break;
                    } catch (TimeoutException e) {
                        // not done yet
                        // check health
                        boolean workerStillRunning = checkWorkerRunning(target);
                        if (!workerStillRunning) {
                            log.warn("Retriggering map/reduce worker since former worker not running anymore.");
                            workerFailures.incrementAndGet();
                            currentReplica = (currentReplica + 1) % replicationFactor;
                            break;
                        } else {
                            log.info("Worker still running.");
                        }
                    }
                }
            }

            log.info("Worker completed.");
        }

        public void complete() {
            responseFuture.complete(null);
        }

        public Range getRange() {
            return range;
        }

        public HostAndPort getRangeOwner() {
            return rangeOwner;
        }

        private CompletableFuture<InitiateMRResponse> federateRequest(HostAndPort node, Range range) {
            InitiateMRRequest federatedRequest = new InitiateMRRequest(request.getId(), range,
                    request.getSourceNamespace(), request.getTargetNamespace(), request.getScript(), myself);

            return CommunicationModule
                    .oneOffMessage(node, federatedRequest)
                    .thenApply(correlatedMessage -> (InitiateMRResponse) correlatedMessage.getMRMessage());
        }

        private boolean checkWorkerRunning(HostAndPort target) {
            MRStatusRequest statusRequest = new MRStatusRequest(request.getId(), MRStatusRequest.Type.WORKER);

            try {
                CorrelatedMessage reply = CommunicationModule
                        .oneOffMessage(target, statusRequest)
                        .get(10, TimeUnit.SECONDS);

                MRStatusMessage status = (MRStatusMessage) reply.getMRMessage();

                if (status.getStatus() == MRStatusMessage.Status.RUNNING) {
                    return true;
                } else {
                    log.warn("Non-running worker status: {}; error: {}", status.getStatus(), status.getError());
                    return false;
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                log.warn("Error while checking worker health.", e);
                return false;
            }
        }

    }

}
