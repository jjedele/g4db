package app_kvEcs;

import app_kvServer.CacheReplacementStrategy;
import client.exceptions.ClientException;
import common.hash.HashRing;
import common.hash.NodeEntry;
import common.hash.Range;
import common.messages.admin.MaintenanceStatusResponse;
import common.messages.gossip.ClusterDigest;
import common.messages.gossip.ServerState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import java.util.stream.Collectors;

/**
 * Default implementation of {@link KVAdmin}.
 */
public class DefaultKVAdmin implements KVAdmin {

    private static final Logger LOG = LogManager.getLogger(DefaultKVAdmin.class);

    /**
     * Simple tuple holding name and address of a server.
     */
    public static class ServerInfo {

        /** Name of the server */
        public final String name;

        /** Username on the server */
        public final String userName;

        /** Address of the server */
        public final InetSocketAddress address;

        /**
         * Default constructor.
         * @param name Name of the server
         * @param userName User name on the server
         * @param address Address of the server
         */
        public ServerInfo(String name, String userName, InetSocketAddress address) {
            this.name = name;
            this.userName = userName;
            this.address = address;
        }

        @Override
        public String toString() {
            return String.format("%s->%s@%s", name, userName, address);
        }
    }


    private class FailureDetector implements Runnable {

        private int samplingSize = 2;
        private final int pauseMs = 2000;

        private double threshold = 16.0;
        private double minStdDeviationMillis = 500;
        private long acceptableHeartbeatPauseMillis = 0;
        private long firstHeartbeatEstimateMillis = 500;

        @Override
        public void run() {
            Map<InetSocketAddress, SimpleFailureDetector> failureDetectorMap = new HashMap<>();

            while (!Thread.interrupted()) {
                List<client.KVAdmin> admins = new ArrayList<>(adminClients.values());
                Collections.shuffle(admins);

                List<CompletableFuture<ClusterDigest>> inFlightRequests = admins.stream().limit(samplingSize)
                        .map(admin -> admin.communicationModule()
                                .send(new ClusterDigest(new HashMap<>()))
                                .thenApply(cm -> (ClusterDigest) cm.getGossipMessage()))
                        .collect(Collectors.toList());

                try {
                    Thread.sleep(pauseMs);
                } catch (InterruptedException e) {
                    // ignore
                }

                Set<ClusterDigest> responses = new HashSet<>();
                inFlightRequests.stream().forEach(future -> {
                    if (future.isDone()) {
                        try {
                            responses.add(future.get());
                        } catch (InterruptedException | ExecutionException e) {
                            LOG.warn(e);
                        }
                    } else {
                        future.cancel(true);
                    }
                });

                // TODO here we have a sample of ClusterDigests and last known heartbeat times, use for failure detection
                LOG.info(responses);
                Set<InetSocketAddress> allNodes = new HashSet<>();

                responses.stream().forEach(clusterDigest -> allNodes.addAll(clusterDigest.getCluster().keySet()));
                allNodes.forEach(node -> {
                    long lastHeartbeat = responses.stream()
                            .mapToLong(digest -> Optional
                                    .ofNullable(digest.getCluster().get(node))
                                    .map(ServerState::getHeartBeat)
                                    .orElse(0l))
                            .max()
                            .getAsLong();

                    SimpleFailureDetector failureDetector = failureDetectorMap.computeIfAbsent(node, node2 -> {
                        LOG.debug("Creating failure detector for node={}", node);
                        return new SimpleFailureDetector(30000);
                    });
                    failureDetector.heartbeat(lastHeartbeat);
                    double suspicion = failureDetector.getSuspicion();
                    LOG.info("Last heartbeat for node {}: {}, suspicion: {}", node, lastHeartbeat, suspicion);

                    if (suspicion >= 1.0) {
                        LOG.info("Decommissioning node={} because believed as dead.", node);

                    }
                });
            }
        }
    }

    private final List<ServerInfo> servers;
    private final Map<InetSocketAddress, client.KVAdmin> adminClients;
    private final HashRing hashRing;
    private Thread failureDetectorThread;

    public DefaultKVAdmin(Collection<ServerInfo> servers) {
        this.servers = new ArrayList<>(servers);
        this.adminClients = new HashMap<>();
        this.hashRing = new HashRing();
        this.failureDetectorThread = new Thread(new FailureDetector());
        failureDetectorThread.start();
    }

    @Override
    public void initService(int numberOfNodes, int cacheSize, CacheReplacementStrategy displacementStrategy) {
        // select n random servers
        List<ServerInfo> candidates = new ArrayList<>(servers);
        //Collections.shuffle(candidates);
        candidates = candidates.stream().limit(numberOfNodes).collect(Collectors.toList());
        LOG.info("Initializing cluster with nodes: {}", candidates);

        // initialize the servers
        final Collection<InetSocketAddress> seedNodes = candidates.stream()
                .map(server -> server.address)
                .collect(Collectors.toSet());

        List<CompletableFuture<Optional<client.KVAdmin>>> inProgressInits = candidates.stream()
                .map(serverInfo -> CompletableFuture.supplyAsync(() ->
                        supplyNode(serverInfo, cacheSize, displacementStrategy, seedNodes)))
                .collect(Collectors.toList());

        try {
            CompletableFuture
                    .allOf(inProgressInits.toArray(new CompletableFuture[] {}))
                    .get(10, TimeUnit.SECONDS);

            inProgressInits.stream()
                    .map(future -> future.getNow(Optional.empty()))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(client -> adminClients.put(client.getNodeAddress(), client));
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            LOG.error("Could not initialize cluster.", e);
        }
    }

    @Override
    public void start() {
        LOG.info("Starting the cluster");
        for (client.KVAdmin connection : adminClients.values()) {
            try {
                connection.start(true);
            } catch (ClientException e) {
                LOG.error("Could not start node " + connection.getNodeAddress(), e);
            }
        }
    }

    @Override
    public void stop() {
        LOG.info("Stopping the cluster");
        for (client.KVAdmin connection : adminClients.values()) {
            try {
                connection.stop();
            } catch (ClientException e) {
                LOG.error("Could not stop node " + connection.getNodeAddress(), e);
            }
        }
    }

    @Override
    public void shutDown() {
        LOG.info("Shutting down the cluster");
        for (client.KVAdmin connection : adminClients.values()) {
            try {
                connection.shutDown();
                connection.disconnect();
            } catch (ClientException e) {
                LOG.error("Could not shut down node " + connection.getNodeAddress(), e);
            }
        }
    }

    @Override
    public void addNode(int cacheSize, CacheReplacementStrategy displacementStrategy) {
        // chose 1 random inactive node
        List<ServerInfo> candidates = servers.stream()
                .filter(serverInfo -> !adminClients.containsKey(serverInfo.address))
                .collect(Collectors.toList());

        if (candidates.size() == 0) {
            throw new RuntimeException("No more nodes to add.");
        }

        //Collections.shuffle(candidates);
        ServerInfo nodeToAdd = candidates.get(0);
        LOG.info("Adding node to cluster: {}", nodeToAdd);

        // set up the node
        Collection<InetSocketAddress> seedNodes = adminClients.keySet().stream()
                .limit(3)
                .collect(Collectors.toSet());
        try {
            client.KVAdmin adminClient = initializeNode(nodeToAdd, cacheSize, displacementStrategy, seedNodes);
            adminClient.start(false);
            adminClients.put(adminClient.getNodeAddress(), adminClient);
            hashRing.addNode(adminClient.getNodeAddress());
        } catch (IOException | InterruptedException | ClientException e) {
            throw new RuntimeException("Could not initialize node.", e);
        }
    }

    @Override
    public void removeNode() {
        // randomly select 1 server to remove
        List<InetSocketAddress> candidates = new ArrayList<>(adminClients.keySet());
        Collections.shuffle(candidates);

        if (candidates.size() == 0) {
            throw new RuntimeException("No active node to remove.");
        }

        InetSocketAddress nodeToRemove = candidates.get(0);

        CompletableFuture<Object> stopFuture = removeNode(nodeToRemove);

        try {
            stopFuture.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.error("Could not decommission node=" + nodeToRemove, e);
        }
    }

    private CompletableFuture<Object> removeNode(InetSocketAddress nodeToBeRemoved) {
        client.KVAdmin candidateAdmin = adminClients.get(nodeToBeRemoved);
        LOG.info("Removing node={} from cluster", nodeToBeRemoved);

        CompletableFuture<Object> stopFuture =
                seedUpdatedStateIntoCluster(nodeToBeRemoved, ServerState.Status.DECOMMISSIONED)
                .whenComplete((res, exc) -> {
                    candidateAdmin.disconnect();
                    adminClients.remove(nodeToBeRemoved);
                });

        return stopFuture;
    }

    private void replaceNode(InetSocketAddress nodeToBeReplaced) {
        removeNode(nodeToBeReplaced)
                .thenCompose(res -> {
                    // wait for a while for the information to propagate through the cluster
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    return CompletableFuture.completedFuture(null);
                })
                .whenComplete((res, exc) -> {
                    // TODO: use actual settings of the failed node
                    addNode(1000, CacheReplacementStrategy.FIFO);
                });
    }

    private CompletableFuture<Object> seedUpdatedStateIntoCluster(InetSocketAddress targetNode, ServerState.Status state) {
        // broadcast state change to target and two other random nodes
        List<InetSocketAddress> candidates = new ArrayList<>(adminClients.keySet());
        Collections.shuffle(candidates);
        candidates = candidates.stream().limit(2).collect(Collectors.toList());
        candidates.add(targetNode);
        List<client.KVAdmin> candidateAdmins = candidates.stream().map(adminClients::get).collect(Collectors.toList());

        // get latest state version
        List<CompletableFuture<ClusterDigest>> inFlightRequests = candidateAdmins.stream()
                .map(admin -> admin.communicationModule()
                    .send(ClusterDigest.empty())
                    .thenApply(reply -> (ClusterDigest) reply.getGossipMessage()))
                .collect(Collectors.toList());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // ignore
        }

        ServerState latestState = inFlightRequests.stream()
                .map(resultFuture -> {
                    if (resultFuture.isDone()) {
                        return resultFuture.getNow(null);
                    } else {
                        resultFuture.cancel(true);
                        return null;
                    }
                })
                .map(digest -> Optional.ofNullable(digest.getCluster().get(targetNode)))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .max(ServerState::compareTo)
                .get();

        LOG.debug("Latest state for {}: {}", targetNode, latestState);

        // inject new state into cluster
        Map<InetSocketAddress, ServerState> updatedCluster = new HashMap<>();
        updatedCluster.put(targetNode, new ServerState(latestState.getGeneration(),
                latestState.getHeartBeat(), state, latestState.getStateVersion() + 1));
        ClusterDigest seedMessage = new ClusterDigest(updatedCluster);

        CompletableFuture[] inFlights = candidateAdmins.stream()
                .map(admin -> admin.communicationModule().send(seedMessage))
                .toArray(CompletableFuture[]::new);

        return CompletableFuture.anyOf(inFlights);
    }

    private Collection<NodeEntry> activeNodeEntries() {
        return servers.stream()
                .filter(serverInfo -> hashRing.contains(serverInfo.address))
                .map(serverInfo -> new NodeEntry(serverInfo.name, serverInfo.address,
                        hashRing.getAssignedRange(serverInfo.address)))
                .collect(Collectors.toList());
    }

    private void moveDataAndWaitForCompletion(InetSocketAddress source, InetSocketAddress destination, Range keyRange)
            throws ClientException {
        client.KVAdmin sourceAdmin = adminClients.get(source);

        LOG.info("Moving data with keys in range {} from {} to {}", keyRange, source, destination);
        sourceAdmin.moveData(destination, keyRange);

        // TODO: add a timeout?
        MaintenanceStatusResponse status = sourceAdmin.getMaintenanceStatus();
        while (status.isActive()) {
            LOG.info("Waiting for completion of {} on node {}. Progress: {}%", status.getTask(), source,
                    status.getProgress());
            status = sourceAdmin.getMaintenanceStatus();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.error("Could not wait in progress polling.", e);
            }
        }
    }

    private static Optional<client.KVAdmin> supplyNode(ServerInfo serverInfo,
                                                       int cacheSize,
                                                       CacheReplacementStrategy displacementStrategy,
                                                       Collection<InetSocketAddress> seedNodes) {
        try {
            return Optional.of(initializeNode(serverInfo, cacheSize, displacementStrategy, seedNodes));
        } catch (IOException | InterruptedException | ClientException e) {
            LOG.error("Could not initialize node.", e);
            return Optional.empty();
        }
    }

    private static client.KVAdmin initializeNode(ServerInfo serverInfo,
                                                 int cacheSize,
                                                 CacheReplacementStrategy displacementStrategy,
                                                 Collection<InetSocketAddress> seedNodes)
            throws IOException, InterruptedException, ClientException {
        // FIXME for now we assume this directory is the same for all nodes
        File workingDir = new File(System.getProperty("user.dir"));

        String joinedSeedNodes = seedNodes.stream()
                .map(address -> String.format("%s:%d", address.getHostString(), address.getPort()))
                .collect(Collectors.joining(","));

        String command = String.format("cd %s && bash ./kv_daemon.sh %d %d %s %s",
                workingDir, serverInfo.address.getPort(), cacheSize, displacementStrategy.name(), joinedSeedNodes);

        executeSSH(serverInfo.address, serverInfo.userName, command);

        client.KVAdmin adminClient = new client.KVAdmin(serverInfo.address);
        adminClient.connect();
        return adminClient;
    }

    private static void executeSSH(InetSocketAddress remoteServer, String remoteUser, String command)
            throws IOException, InterruptedException {
        String[] cmd = new String[] {
                "ssh",
                String.format("%s@%s", remoteUser, remoteServer.getHostString()),
                command
        };
        Runtime runtime = Runtime.getRuntime();
        Process process = runtime.exec(cmd);
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("Process exited with error code: " + exitCode);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        DefaultKVAdmin kvadmin = new DefaultKVAdmin(Arrays.asList(
                new ServerInfo("node1", "xhens", new InetSocketAddress("localhost", 50000)),
                new ServerInfo("node2", "xhens", new InetSocketAddress("localhost", 50001)),
                new ServerInfo("node3", "xhens", new InetSocketAddress("localhost", 50002)),
                new ServerInfo("node4", "xhens", new InetSocketAddress("localhost", 50003))));
        kvadmin.initService(3, 45, CacheReplacementStrategy.LRU);
        kvadmin.start();
//        Thread.sleep(20000);
//        kvadmin.addNode(25, CacheReplacementStrategy.FIFO);
//        Thread.sleep(10000);
//        kvadmin.removeNode();
//        Thread.sleep(30000);
//        kvadmin.shutDown();
    }
}
