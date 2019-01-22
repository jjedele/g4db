package app_kvServer.mapreduce;

import app_kvServer.gossip.Gossiper;
import client.CommunicationModule;
import client.KVStore;
import client.exceptions.ClientException;
import common.hash.HashRing;
import common.hash.Range;
import common.messages.mapreduce.InitiateMRRequest;
import common.messages.mapreduce.InitiateMRResponse;
import common.messages.mapreduce.MRStatusMessage;
import common.utils.ContextPreservingThread;
import common.utils.FutureUtils;
import common.utils.HostAndPort;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.script.ScriptException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
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

    private final Set<Range> pendingResults;
    private final Set<Range> finishedResults;
    private final MapReduceProcessor resultProcessor;

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

        this.pendingResults = new CopyOnWriteArraySet<>();
        this.finishedResults = new CopyOnWriteArraySet<>();

        this.resultProcessor = new MapReduceProcessor(request.getScript());
    }

    /**
     * Run the map/reduce process.
     */
    @Override
    public void run() {
        setUpThreadContext();
        LOG.info("Starting map/reduce master process with id={}.", request.getId());

        HashRing ring = new HashRing(Gossiper.getInstance().getClusterDigest().getCluster().keySet());

        ring.getNodes().stream()
                .map(ring::getAssignedRange)
                .forEach(pendingResults::add);

        List<CompletableFuture<InitiateMRResponse>> responses = ring.getNodes().stream()
                .map(node -> federateRequest(node, ring.getAssignedRange(node)))
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
            if (pendingResults.contains(range)) {
                LOG.info("Received data from completed m/r worker for job={}, range={}",
                        request.getId(), range);

                results.entrySet().stream()
                        .forEach(e -> resultProcessor.processMapResult(e.getKey(), e.getValue()));
                pendingResults.remove(range);
                finishedResults.add(range);

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
        long extentPending = pendingResults.stream().mapToLong(Range::getExtent).sum();
        long extentComplete = finishedResults.stream().mapToLong(Range::getExtent).sum();
        int percentageComplete = (int) Math.round(100.0 * extentComplete / (extentComplete + extentPending));
        MRStatusMessage.Status status =
                pendingResults.isEmpty() ? MRStatusMessage.Status.FINISHED : MRStatusMessage.Status.RUNNING;
        String error = null;

        return new MRStatusMessage(id, status, workersTotal, workersComplete, workersFailed, percentageComplete, error);
    }

    private CompletableFuture<InitiateMRResponse> federateRequest(HostAndPort node, Range range) {
        InitiateMRRequest federatedRequest = new InitiateMRRequest(request.getId(), range,
                request.getSourceNamespace(), request.getTargetNamespace(), request.getScript(), myself);

        try {
            CommunicationModule communicationModule = new CommunicationModule(node);
            communicationModule.start();

            return communicationModule
                    .send(federatedRequest)
                    .thenApply(reply -> (InitiateMRResponse) reply.getMRMessage())
                    .whenComplete((res, exc) -> communicationModule.stop());
        } catch (ClientException e) {
            return FutureUtils.failedFuture(e);
        }
    }

    private void finishJob() {
        Map<String, String> results = resultProcessor.getResults();
        LOG.info("M/r job={} finished, result size={}.", request.getId(), results.entrySet().size());

        KVStore kvStore = new KVStore(myself.getHost(), myself.getPort());
        try {
            kvStore.connect();

            for (Map.Entry<String, String> e : results.entrySet()) {
                kvStore.put(String.format("%s/%s", request.getTargetNamespace(), e.getKey()), e.getValue());
            }
        } catch (ClientException e) {
            LOG.error("Could not persist m/r results for job=" + request.getId(), e);
        } finally {
            kvStore.disconnect();
        }
    }

}
