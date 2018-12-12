package app_kvServer.sync;

import app_kvServer.gossip.Gossiper;
import client.CommunicationModule;
import client.exceptions.ClientException;
import common.hash.HashRing;
import common.hash.Range;
import common.messages.admin.InitiateStreamRequest;
import common.messages.admin.InitiateStreamResponse;
import common.messages.gossip.ClusterDigest;
import common.messages.gossip.ServerState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * The Synchronizer is responsible for bootstrapping and redistributing data after a node was
 * decomissioned.
 *
 * It is a singleton per server.
 */
public class Synchronizer {

    private static final Logger LOG = LogManager.getLogger(Synchronizer.class);
    private static Synchronizer instance;

    private final InetSocketAddress myself;
    private final app_kvServer.ServerState serverState;
    private final Map<Range, CompletableFuture<Void>> streamFutures;

    private Synchronizer(app_kvServer.ServerState serverState) {
        this.myself = serverState.getMyself();
        this.serverState = serverState;
        this.streamFutures = new ConcurrentHashMap<>();
    }

    /**
     * Initialize the synchronizer for this node.
     *
     * This method is not thread-safe and must be called accordingly.
     * @param serverState Server state
     */
    public static void initialize(app_kvServer.ServerState serverState) {
        if (instance != null) {
            throw new IllegalStateException("Already initialized.");
        }

        instance = new Synchronizer(serverState);
    }

    /**
     * Return the singleton instance of the synchronizer.
     * @return Instance
     */
    public static Synchronizer getInstance() {
        if (instance == null) {
            throw new IllegalStateException("Must be initialized first.");
        }

        return instance;
    }

    /**
     * Join the local node into an existing cluster.
     */
    public CompletableFuture<Void> initiateJoin() {
        LOG.info("Initiating join.");
        ClusterDigest clusterDigest = Gossiper.getInstance().getClusterDigest();

        while (clusterDigest.getCluster().isEmpty()) {
            try {
                LOG.info("Waiting for cluster data to arrive.");
                Thread.sleep(2000);
                clusterDigest = Gossiper.getInstance().getClusterDigest();
            } catch (InterruptedException e) {
                LOG.warn("Could not wait.", e);
            }
        }

        Collection<InetSocketAddress> aliveNodes = clusterDigest.getCluster().entrySet().stream()
                .filter(entry -> entry.getValue().getStatus() != ServerState.Status.DECOMMISSIONED)
                .map(entry -> entry.getKey())
                .collect(Collectors.toList());

        Gossiper.getInstance().setOwnState(ServerState.Status.JOINING);
        HashRing ring = new HashRing(aliveNodes);
        serverState.setClusterNodes(aliveNodes);

        // primary range takeover
        Range myRange = ring.getAssignedRange(myself);
        InetSocketAddress currentMyRangeOwner = ring.getSuccessor(myself);

        // replicas
        InetSocketAddress firstPredecessor = ring.getPredecessor(myself, 1);
        Range firstReplicaRange = ring.getAssignedRange(firstPredecessor);

        InetSocketAddress secondPredecessor = ring.getPredecessor(myself, 2);
        Range secondReplicaRange = ring.getAssignedRange(secondPredecessor);

        StringJoiner logJoiner = new StringJoiner("\n", "JoinPlan[\n", "]");
        logJoiner.add(String.format("PR: %s from %s", myRange, currentMyRangeOwner));
        logJoiner.add(String.format("R1: %s from %s", firstReplicaRange, firstPredecessor));
        logJoiner.add(String.format("R2: %s from %s", secondReplicaRange, secondPredecessor));

        LOG.info("Join initiated: {}", logJoiner.toString());

        CompletableFuture<Void> prTransfer = CompletableFuture.runAsync(new StreamSentinel(myRange, Arrays.asList(currentMyRangeOwner)));
        CompletableFuture<Void> r1Transfer = CompletableFuture.runAsync(new StreamSentinel(firstReplicaRange, Arrays.asList(firstPredecessor)));
        CompletableFuture<Void> r2Transfer = CompletableFuture.runAsync(new StreamSentinel(secondReplicaRange, Arrays.asList(secondPredecessor)));

        CompletableFuture<Void> allTransfers = CompletableFuture.allOf(prTransfer, r1Transfer, r2Transfer);

        allTransfers = allTransfers.whenComplete((result, exc) -> Gossiper.getInstance().setOwnState(ServerState.Status.OK));

        return allTransfers;
    }

    /**
     * Handle a {@link common.messages.admin.StreamCompleteMessage} from another server.
     * @param range
     */
    public void streamCompleted(Range range) {
        CompletableFuture<Void> future = streamFutures.get(range);
        if (future == null) {
            LOG.debug("Got stream completion message for unknown stream.");
            return;
        } else {
            future.complete(null);
        }
    }

    private class StreamSentinel implements Runnable {

        private final Range range;
        private final List<InetSocketAddress> providerCandidates;

        public StreamSentinel(Range range, List<InetSocketAddress> providerCandidates) {
            this.range = range;
            this.providerCandidates = providerCandidates;
        }

        @Override
        public void run() {
            ThreadContext.put("serverPort", Integer.toString(myself.getPort()));
            InetSocketAddress source = providerCandidates.get(0);
            CommunicationModule communicationModule = null;
            try {
                communicationModule = new CommunicationModule(source, 100);
                communicationModule.start();

                // TODO real
                InitiateStreamRequest request = new InitiateStreamRequest(myself, range,
                        Gossiper.getInstance().getClusterDigest(), false);

                CompletableFuture<Void> streamEnd = new CompletableFuture<>();
                streamFutures.put(range, streamEnd);

                InitiateStreamResponse response = communicationModule
                        .send(request)
                        .thenApply(reply -> ((InitiateStreamResponse) reply.getAdminMessage()))
                        .get();

                LOG.info("Initiated stream: {}", response.getStreamId());

                streamEnd.get();

                LOG.info("Completed stream: {}", response.getStreamId());
            } catch (ClientException | InterruptedException | ExecutionException e) {
                LOG.error("Could not initiate data stream.", e);
                throw new RuntimeException("Could not initiate data stream.", e);
            } finally {
                if (communicationModule != null) {
                    communicationModule.stop();
                }
            }
        }
    }

}
