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

        private final int samplingSize = 2;
        private final int pauseMs = 2000;

        @Override
        public void run() {
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
                    LOG.info("Last heartbeat for node {}: {}", node, lastHeartbeat);
                    // TODO we can probably use some simple form of accrual failure detector here
                    // TODO: read http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.80.7427&rep=rep1&type=pdf for an overview
                    // TODO: ignore the network condition stuff for now, keep it simple
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
        candidates.stream()
                .map(serverInfo -> {
                    Optional<client.KVAdmin> adminClient = Optional.empty();
                    try {
                        adminClient = Optional.of(initializeNode(serverInfo, cacheSize, displacementStrategy, seedNodes));
                    } catch (IOException | InterruptedException | ClientException e) {
                        LOG.error("Could not start node.", e);
                    }
                    return adminClient;
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(adminClient -> {
                    adminClients.put(adminClient.getNodeAddress(), adminClient);
                    hashRing.addNode(adminClient.getNodeAddress());
                });
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
        client.KVAdmin candidateAdmin = adminClients.get(nodeToRemove);
        LOG.info("Removing node from cluster: {}", nodeToRemove);

        // transfer data from node to be removed to successor
        InetSocketAddress successor = hashRing.getSuccessor(nodeToRemove);
        Range assignedRange = hashRing.getAssignedRange(nodeToRemove);

        hashRing.removeNode(nodeToRemove);

        try {
            // we need to update the meta data for the successor before starting the transfer so that it accepts the entries
            adminClients.get(successor).updateMetadata(activeNodeEntries());
            candidateAdmin.enableWriteLock();
            moveDataAndWaitForCompletion(nodeToRemove, successor, assignedRange);
            candidateAdmin.disableWriteLock();
        } catch (ClientException e) {
            // break and let the admin fix it
            throw new RuntimeException("Could not transfer data from removed node.", e);
        }

        // shutdown the node
        try {
            candidateAdmin.shutDown();
        } catch (ClientException e) {
            LOG.error("Could not shutdown node: " + nodeToRemove, e);
        }
        adminClients.remove(nodeToRemove);

        // notify other nodes about the updated cluster
        Collection<NodeEntry> clusterNodes = activeNodeEntries();
        LOG.info("Informing nodes about new cluster state: {}", clusterNodes);
        adminClients.values().stream()
                .forEach(adminClient -> {
                    try {
                        adminClient.updateMetadata(clusterNodes);
                    } catch (ClientException e) {
                        LOG.error("Could not updated metadata on node " + adminClient.getNodeAddress(), e);
                    }
                });
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
                new ServerInfo("node1", "jeff", new InetSocketAddress("localhost", 50000)),
                new ServerInfo("node2", "jeff", new InetSocketAddress("localhost", 50001)),
                new ServerInfo("node3", "jeff", new InetSocketAddress("localhost", 50002)),
                new ServerInfo("node4", "jeff", new InetSocketAddress("localhost", 50003))));
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
