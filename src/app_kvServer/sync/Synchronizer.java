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
import common.utils.HostAndPort;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The Synchronizer is responsible for bootstrapping and redistributing data after a node was
 * decomissioned.
 *
 * It is a singleton per server.
 */
public class Synchronizer {

    private static final Logger LOG = LogManager.getLogger(Synchronizer.class);
    private static Synchronizer instance;

    private final int replicationFactor;
    private final HostAndPort myself;
    private final app_kvServer.ServerState serverState;
    private final Map<Range, CompletableFuture<Void>> streamFutures;

    private Synchronizer(app_kvServer.ServerState serverState) {
        this.replicationFactor = 3;
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
        LOG.info("Initiating joining process for node={}.", myself);

        ClusterDigest clusterDigest = Gossiper.getInstance().getClusterDigest();

        Collection<HostAndPort> aliveNodes = clusterDigest.getCluster().entrySet().stream()
                .filter(entry -> entry.getValue().getStatus() != ServerState.Status.DECOMMISSIONED)
                .map(entry -> entry.getKey())
                .collect(Collectors.toList());

        Gossiper.getInstance().setOwnState(ServerState.Status.JOINING);
        HashRing ring = new HashRing(aliveNodes);
        serverState.setClusterNodes(aliveNodes);

        // assemble synchronization plan
        SynchronizationPlan synchronizationPlan = new SynchronizationPlan("JOIN");

        IntStream
                .range(0, replicationFactor)
                .forEach(predecessorNo -> {
                    // for 0 that's us
                    HostAndPort predecessor = ring.getPredecessor(myself, predecessorNo);
                    Range wantedKeyRange = ring.getAssignedRange(predecessor);
                    HostAndPort[] sources = IntStream
                            .rangeClosed(-predecessorNo, replicationFactor-predecessorNo)
                            .filter(i -> i != 0) // can't copy anything from our own node
                            .mapToObj(i -> ring.traverse(myself, i))
                            .toArray(HostAndPort[]::new);

                    synchronizationPlan.add(wantedKeyRange, sources);
                });

        CompletableFuture<Void> synchronization = executeSynchronizationPlan(synchronizationPlan)
                .whenComplete((result, exc) -> Gossiper.getInstance().setOwnState(ServerState.Status.OK));

        return synchronization;
    }

    public CompletableFuture<Void> initiateDecommissioning(HostAndPort decommissionedNode) {
        LOG.info("Initiating decommissioning process for node={}", decommissionedNode);

        ClusterDigest clusterDigest = Gossiper.getInstance().getClusterDigest();
        Set<HostAndPort> consideredNodes = clusterDigest.getCluster().entrySet().stream()
                .filter(entry -> entry.getValue().getStatus() != ServerState.Status.DECOMMISSIONED)
                .map(entry -> entry.getKey())
                .collect(Collectors.toSet());

        // need the decommissioned node in here to reallocate the data
        consideredNodes.add(decommissionedNode);

        HashRing ring = new HashRing(consideredNodes);

        Optional<SynchronizationPlan> synchronizationPlanOption = Optional.empty();

        int successorNumber = ring.findSuccessorNumber(decommissionedNode, myself);

        if (successorNumber <= replicationFactor) {
            // we're impacted by the decommissioning
            Gossiper.getInstance().setOwnState(ServerState.Status.REBALANCING);

            SynchronizationPlan synchronizationPlan =
                    new SynchronizationPlan("DECOMMISSION_TAKEOVER_SUCCESSOR_" + successorNumber);

            Range missingRange = ring.getAssignedRange(ring.getPredecessor(myself, replicationFactor));

            HostAndPort[] sources = IntStream
                    .rangeClosed(1, replicationFactor)
                    .map(i -> replicationFactor - i + 1) // reverse to get priorities right
                    .mapToObj(predecessorNo -> ring.getPredecessor(myself, predecessorNo))
                    .filter(node -> !decommissionedNode.equals(node))
                    .toArray(HostAndPort[]::new);

            synchronizationPlan.add(
                    missingRange,
                    sources);

            synchronizationPlanOption = Optional.of(synchronizationPlan);
        }

        return synchronizationPlanOption
                .map(this::executeSynchronizationPlan)
                .map(future -> future.whenComplete((result, exc) -> {
                    Gossiper.getInstance().setOwnState(ServerState.Status.OK);
                }))
                .orElseGet(() -> CompletableFuture.completedFuture(null));
    }

    private CompletableFuture<Void> executeSynchronizationPlan(SynchronizationPlan synchronizationPlan) {
        LOG.info("Executing synchronization for reason={}:\n{}",
                synchronizationPlan.getReason(), synchronizationPlan);

        CompletableFuture[] inProgressStreams = synchronizationPlan.getDataPartitions().entrySet().stream()
                .filter(e -> !e.getValue().get(0).equals(myself))
                .map(e -> new StreamSentinel(e.getKey(), e.getValue()))
                .map(stream -> CompletableFuture.runAsync(stream))
                .toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(inProgressStreams);
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
        private final List<HostAndPort> providerCandidates;

        public StreamSentinel(Range range, List<HostAndPort> providerCandidates) {
            this.range = range;
            this.providerCandidates = providerCandidates;
        }

        @Override
        public void run() {
            ThreadContext.put("serverPort", Integer.toString(myself.getPort()));
            HostAndPort source = providerCandidates.get(0);
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
