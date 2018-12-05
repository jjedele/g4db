package app_kvServer.gossip;

import common.CorrelatedMessage;
import common.Protocol;
import common.messages.gossip.ServerState;
import common.messages.gossip.ClusterDigest;
import common.utils.RecordReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static common.Protocol.RECORD_SEPARATOR;

/**
 * The Gossiper is responsible for server-to-server gossip communication in order to communicate
 * the cluster state.
 *
 * It is a singleton instance per server.
 */
public class Gossiper {

    private static final Logger LOG = LogManager.getLogger(Gossiper.class);
    private static Gossiper instance;

    private final InetSocketAddress myself;
    private final long generation;
    private long heartbeat;
    private final Map<InetSocketAddress, ServerState> cluster;
    private final Set<InetSocketAddress> seedNodes;
    private final ScheduledExecutorService executorService;
    private ServerState.Status ownState;
    private final Set<InetSocketAddress> deadCandidates;
    private final Random random;
    private final Collection<GossipEventListener> eventListeners;
    private final int gossipIntervalSeconds;

    /**
     * Initialize the Gossiper for this server instance.
     *
     * This method is not thread-safe and should be called accordingly.
     *
     * @param myself Address to which the server instance is bound
     */
    public static void initialize(InetSocketAddress myself) {
        if (instance != null) {
            throw new IllegalStateException("Gossiper already initialized.");
        }

        instance = new Gossiper(myself);
        Arrays.stream(System.getProperty("seedNodes", "").split(",")).forEach(nodeStr -> {
            String[] parts = nodeStr.split(":");
            InetSocketAddress node = new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
            instance.addSeed(node);
        });
        instance.start();
    }

    /**
     * Return the initialized Gossiper instance.
     *
     * @return Gossiper instance
     */
    public static Gossiper getInstance() {
        if (instance == null) {
            throw new IllegalStateException("Gossiper must be initialized first.");
        }
        return instance;
    }

    /**
     * Start the gossiping.
     */
    public void start() {
        executorService.scheduleAtFixedRate(new GossipTask(), 1, gossipIntervalSeconds, TimeUnit.SECONDS);
    }

    /**
     * Stopping the gossiping.
     */
    public void stop() {
        executorService.shutdownNow();
    }

    /**
     * Add a seed node.
     *
     * These will be contacted first to get information about other nodes in the cluster.
     *
     * @param seedNode
     */
    public void addSeed(InetSocketAddress seedNode) {
        seedNodes.add(seedNode);
    }

    /**
     * Register a GossipEventListener to be notified when the cluster state changes.
     *
     * @param listener
     */
    public void addListener(GossipEventListener listener) {
        this.eventListeners.add(listener);
    }

    /**
     * Return the current view of the cluster.
     *
     * @return
     */
    public ClusterDigest getClusterDigest() {
        return new ClusterDigest(cluster);
    }

    /**
     * Merge the cluster information we got from some other node with the locally known version.
     * @param other Cluster information from other node
     * @return Updated cluster information
     */
    public synchronized ClusterDigest handleIncomingDigest(ClusterDigest other) {
        Map<InetSocketAddress, ServerState> otherCluster = other.getCluster();

        // nodes the other one knows and we don't we simply add to our state of the cluster
        Set<InetSocketAddress> toAdd = new HashSet<>(otherCluster.keySet());
        toAdd.removeAll(cluster.keySet());

        toAdd.stream().forEach(node -> {
            cluster.put(node, otherCluster.get(node));
        });

        // nodes we both know we have to merge accordingly
        Set<InetSocketAddress> toMerge = new HashSet<>(otherCluster.keySet());
        toMerge.retainAll(cluster.keySet());

        Set<InetSocketAddress> othersMoreRecent = toMerge.stream()
                .filter(node -> cluster.get(node).compareTo(otherCluster.get(node)) < 0)
                .collect(Collectors.toSet());

        Map<InetSocketAddress, ServerState.Status> changes = new HashMap<>();

        othersMoreRecent.stream().forEach(node -> {
            ServerState localState = cluster.get(node);
            ServerState remoteState = otherCluster.get(node);
            if (localState.getStatus() != remoteState.getStatus()) {
                changes.put(node, remoteState.getStatus());
            }
            cluster.put(node, remoteState);
        });

        // TODO we could reduce the traffic a little by only sending back what the other really needs
        //Set<InetSocketAddress> oursMoreRecent = toMerge.stream()
        //        .filter(node -> cluster.get(node).compareTo(otherCluster.get(node)) > 0)
        //        .collect(Collectors.toSet());

        // handle event listeners
        for (GossipEventListener listener : eventListeners) {
            for (InetSocketAddress node : toAdd) {
                listener.nodeAdded(node);
            }

            for (Map.Entry<InetSocketAddress, ServerState.Status> change : changes.entrySet()) {
                listener.nodeChanged(change.getKey(), change.getValue());
            }

            if (!toAdd.isEmpty() || !changes.isEmpty()) {
                listener.clusterChanged(new ClusterDigest(cluster));
            }
        }

        return new ClusterDigest(cluster);
    }

    // only to be accessed via singleton pattern
    private Gossiper(InetSocketAddress myself) {
        this.myself = myself;
        this.generation = System.currentTimeMillis();
        this.heartbeat = 0;
        this.cluster = new HashMap<>();
        this.seedNodes = new HashSet<>();
        this.executorService = new ScheduledThreadPoolExecutor(2);
        this.ownState = ServerState.Status.JOINING;
        this.deadCandidates = new HashSet<>();
        this.random = new Random();
        this.eventListeners = new HashSet<>();
        this.gossipIntervalSeconds = 1;
    }

    // the efficiency of of gossiping improves if we select the nodes we contact with some care
    private Collection<InetSocketAddress> selectGossipTargets() {
        // parameters
        final int fanOut = 1;
        final double contactSeedNodeChance = 0.3;
        final double contactDeadNodeChance = 0.3;

        // we try to contact <fanOut> regular nodes in each round
        List<InetSocketAddress> candidates = new ArrayList<>(cluster.keySet());
        Collections.shuffle(candidates);
        List<InetSocketAddress> targets = candidates.stream()
                .filter(node -> !myself.equals(node) && !deadCandidates.contains(node))
                .limit(fanOut)
                .collect(Collectors.toList());

        // seed node handling
        // if we don't know about other nodes we definitely contact a seed node
        // else we probabilistically contact a seed node additionally to have some information hotspots
        if (targets.isEmpty() || random.nextFloat() < contactSeedNodeChance) {
            List<InetSocketAddress> seeds = new ArrayList<>(seedNodes);
            Collections.shuffle(seeds);
            targets.add(seeds.get(0));
        }

        // dead node handling
        // we probabilistically retry to reach a potentially dead node to find out if they came back to life
        if (!deadCandidates.isEmpty() && random.nextFloat() < contactDeadNodeChance) {
            List<InetSocketAddress> deadNodes = new ArrayList<>(deadCandidates);
            Collections.shuffle(deadNodes);
            targets.add(deadNodes.get(0));
        }

        return targets;
    }

    private class GossipTask implements Runnable {
        @Override
        public void run() {
            ThreadContext.put("serverPort", Integer.toString(myself.getPort()));
            heartbeat = System.currentTimeMillis() - generation;

            // update our own state
            // TODO DECOMISSIONED states should always prevail (as long generation is the same)
            cluster.put(myself, new ServerState(generation, heartbeat, ownState, heartbeat));
            ClusterDigest digest = new ClusterDigest(cluster);
            LOG.debug("Gossip digest: {}", digest);

            // initiate a gossip exchange with a small number of cluster nodes
            Map<InetSocketAddress, Future<ClusterDigest>> inFlightRequests = new HashMap<>();
            for (InetSocketAddress target : selectGossipTargets()) {
                inFlightRequests.put(target,
                        executorService.schedule(
                                new GossipExchange(target), 0, TimeUnit.SECONDS));
            }

            // resolve the outcomes of the gossip exchanges
            for (Map.Entry<InetSocketAddress, Future<ClusterDigest>> request : inFlightRequests.entrySet()) {
                try {
                    digest = request.getValue().get(2, TimeUnit.SECONDS);
                    handleIncomingDigest(digest);
                    deadCandidates.remove(request.getKey());
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    LOG.debug("Could not gossip with node " + request.getKey(), e);
                    deadCandidates.add(request.getKey());
                }
            }
        }
    }

    private class GossipExchange implements Callable<ClusterDigest> {

        private final InetSocketAddress target;

        public GossipExchange(InetSocketAddress target) {
            this.target = target;
        }

        @Override
        public ClusterDigest call() throws Exception {
            Socket socket = new Socket(target.getAddress(), target.getPort());

            try {
                // send the message
                OutputStream os = socket.getOutputStream();
                byte[] payloadOut = Protocol.encode(new ClusterDigest(cluster), heartbeat);
                os.write(payloadOut);
                os.write(RECORD_SEPARATOR);

                // receive the reply
                InputStream is = socket.getInputStream();
                RecordReader recordReader = new RecordReader(is, RECORD_SEPARATOR);
                byte[] payloadIn = recordReader.read();
                CorrelatedMessage correlatedMessage = Protocol.decode(payloadIn);

                return (ClusterDigest) correlatedMessage.getGossipMessage();
            } finally {
                socket.close();
            }
        }

    }

}
