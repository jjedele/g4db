package app_kvEcs;

import app_kvServer.CacheReplacementStrategy;
import client.KVAdminInterface;
import client.exceptions.ClientException;
import common.hash.HashRing;
import common.hash.NodeEntry;
import common.hash.Range;
import common.messages.admin.MaintenanceStatusResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
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

    private final List<ServerInfo> servers;
    private final Map<InetSocketAddress, KVAdminInterface> adminClients;
    private final HashRing hashRing;

    public DefaultKVAdmin(Collection<ServerInfo> servers) {
        this.servers = new ArrayList<>(servers);
        this.adminClients = new HashMap<>();
        this.hashRing = new HashRing();
    }

    @Override
    public void initService(int numberOfNodes, int cacheSize, CacheReplacementStrategy displacementStrategy) {
        // select n random servers
        List<ServerInfo> candidates = new ArrayList<>(servers);
        Collections.shuffle(candidates);
        candidates = candidates.stream().limit(numberOfNodes).collect(Collectors.toList());
        LOG.info("Initializing cluster with nodes: {}", candidates);

        // initialize the servers
        candidates.stream()
                .map(serverInfo -> {
                    Optional<KVAdminInterface> adminClient = Optional.empty();
                    try {
                        adminClient = Optional.of(initializeNode(serverInfo, cacheSize, displacementStrategy));
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

        // compose cluster metadata and send it to servers
        Collection<NodeEntry> activeNodes = activeNodeEntries();
        LOG.info("Updating cluster nodes with following membership information: {}", activeNodes);
        adminClients.values().stream()
                .forEach(adminClient -> {
                    try {
                        adminClient.updateMetadata(activeNodes);
                    } catch (ClientException e) {
                        LOG.error("Could not updated metadata on node " + adminClient.getNodeAddress(), e);
                    }
                });
    }

    @Override
    public void start() {
        LOG.info("Starting the cluster");
        for (KVAdminInterface connection : adminClients.values()) {
            try {
                connection.start();
            } catch (ClientException e) {
                LOG.error("Could not start node " + connection.getNodeAddress(), e);
            }
        }
    }

    @Override
    public void stop() {
        LOG.info("Stopping the cluster");
        for (KVAdminInterface connection : adminClients.values()) {
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
        for (KVAdminInterface connection : adminClients.values()) {
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

        Collections.shuffle(candidates);
        ServerInfo nodeToAdd = candidates.get(0);
        LOG.info("Adding node to cluster: {}", nodeToAdd);

        // set up the node
        try {
            KVAdminInterface adminClient = initializeNode(nodeToAdd, cacheSize, displacementStrategy);
            adminClient.start();
            adminClients.put(adminClient.getNodeAddress(), adminClient);
            hashRing.addNode(adminClient.getNodeAddress());
        } catch (IOException | InterruptedException | ClientException e) {
            throw new RuntimeException("Could not initialize node.", e);
        }

        // transfer data from the successor to the new node
        InetSocketAddress successor = hashRing.getSuccessor(nodeToAdd.address);
        Range assignedRange = hashRing.getAssignedRange(nodeToAdd.address);
        try {
            KVAdminInterface successorAdmin = adminClients.get(successor);
            successorAdmin.enableWriteLock();
            moveDataAndWaitForCompletion(successor, nodeToAdd.address, assignedRange);
            successorAdmin.disableWriteLock();
        } catch (ClientException e) {
            LOG.error("Could not transfer data to new node.", e);
        }

        // notify other nodes about the updated cluster
        // this will also cause the successor of the new node to clean up the transferred data
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

    @Override
    public void removeNode() {
        // randomly select 1 server to remove
        List<InetSocketAddress> candidates = new ArrayList<>(adminClients.keySet());
        Collections.shuffle(candidates);

        if (candidates.size() == 0) {
            throw new RuntimeException("No active node to remove.");
        }

        InetSocketAddress nodeToRemove = candidates.get(0);
        KVAdminInterface candidateAdmin = adminClients.get(nodeToRemove);
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
        KVAdminInterface sourceAdmin = adminClients.get(source);

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

    private static KVAdminInterface initializeNode(ServerInfo serverInfo,
                                                   int cacheSize,
                                                   CacheReplacementStrategy displacementStrategy)
            throws IOException, InterruptedException, ClientException {
        // FIXME for now we assume this directory is the same for all nodes
        File workingDir = new File(System.getProperty("user.dir"));

        String command = String.format("cd %s && bash ./kv_daemon.sh %d %d %s",
                workingDir, serverInfo.address.getPort(), cacheSize, displacementStrategy.name());

        executeSSH(serverInfo.address, serverInfo.userName, command);

        KVAdminInterface adminClient = new client.KVAdmin(serverInfo.address);
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
                new ServerInfo("node2", "jeff", new InetSocketAddress("localhost", 50000))));
        kvadmin.initService(1, 45, CacheReplacementStrategy.LRU);
        kvadmin.start();
        Thread.sleep(20000);
        kvadmin.addNode(25, CacheReplacementStrategy.FIFO);
        Thread.sleep(10000);
        kvadmin.removeNode();
        Thread.sleep(30000);
        kvadmin.shutDown();
    }
}
