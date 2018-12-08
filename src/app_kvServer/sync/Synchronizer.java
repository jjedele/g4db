package app_kvServer.sync;

import app_kvServer.gossip.Gossiper;
import common.hash.HashRing;
import common.hash.Range;
import common.messages.gossip.ClusterDigest;
import common.messages.gossip.ServerState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.StringJoiner;

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

    private Synchronizer(InetSocketAddress myself) {
        this.myself = myself;
    }

    /**
     * Initialize the synchronizer for this node.
     *
     * This method is not thread-safe and must be called accordingly.
     * @param myself The address to which this server node is bound
     */
    public static void initialize(InetSocketAddress myself) {
        if (instance != null) {
            throw new IllegalStateException("Already initialized.");
        }

        instance = new Synchronizer(myself);
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
    public void initiateJoin() {
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

        Gossiper.getInstance().setOwnState(common.messages.gossip.ServerState.Status.JOINING);
        HashRing ring = new HashRing(clusterDigest.getCluster().keySet());

        // primary range takeover
        Range myRange = ring.getAssignedRange(myself);
        InetSocketAddress currentMyRangeOwner = ring.getSuccessor(myself);

        // replicas
        InetSocketAddress firstPredecessor = ring.getPredecessor(myself, 1);
        Range firstReplicaRange = ring.getAssignedRange(firstPredecessor);

        InetSocketAddress secondPredecessor = ring.getPredecessor(myself, 2);
        Range secondReplicaRange = ring.getAssignedRange(secondPredecessor);

        StringJoiner joiner = new StringJoiner("\n", "JoinPlan[\n", "]");
        joiner.add(String.format("PR: %s from %s", myRange, currentMyRangeOwner));
        joiner.add(String.format("R1: %s from %s", firstReplicaRange, firstPredecessor));
        joiner.add(String.format("R2: %s from %s", secondReplicaRange, secondPredecessor));

        Gossiper.getInstance().setOwnState(ServerState.Status.JOINING);
        LOG.info("Join initiated: {}", joiner.toString());
    }


}
