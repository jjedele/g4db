package app_kvEcs;

import app_kvServer.CacheReplacementStrategy;
import app_kvServer.persistence.Cache;
import app_kvServer.persistence.FIFOCache;
import client.DummyAdminClient;
import client.KVAdminInterface;
import client.exceptions.ClientException;
import com.jcraft.jsch.JSchException;
import common.hash.HashRing;
import common.hash.NodeEntry;
import common.hash.Range;
import common.messages.admin.MaintenanceStatusResponse;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link KVAdmin}.
 */
public class DefaultKVAdmin implements KVAdmin {

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

    }

    private final List<ServerInfo> servers;
    private final Map<InetSocketAddress, KVAdminInterface> adminClients;

    public DefaultKVAdmin(Collection<ServerInfo> servers) {
        // TODO: maintain list of possible server nodes - DONE
        this.servers = new ArrayList<>(servers);
        this.adminClients = new HashMap<>();
    }

    @Override
    public void initService(int numberOfNodes, int cacheSize, CacheReplacementStrategy displacementStrategy) {
        // start servers
        String workingDir = System.getProperty("user.dir");
        HashRing hashRing = new HashRing();
        for (int i = 0; i < numberOfNodes; i++) {
            ServerInfo currentServer = servers.get(i);
            System.out.println("Servers: " + servers.get(i));
            String command = String.format("bash nohup java -jar %s/ms3-server.jar %d %d %s &",
                    workingDir, currentServer.address.getPort(), cacheSize, displacementStrategy.name());
            System.out.println(command);
            try {
                executeSSH(currentServer.address, currentServer.userName, command);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }

            hashRing.addNode(currentServer.address);
            KVAdminInterface adminClient = new DummyAdminClient(currentServer.address);
            try {
                adminClient.connect();
            } catch (ClientException e) {
                e.printStackTrace();
            }
            adminClients.put(currentServer.address, adminClient);
        }

        // compose cluster metadata and send it to servers
        List<NodeEntry> clusterState = new LinkedList<>();
        for (InetSocketAddress server : hashRing.getNodes()) {
            // TODO server name and key range do not need to be sent, remove at some point
            clusterState.add(new NodeEntry("somename", server, new Range(0, 1)));
        }
        for (KVAdminInterface serverAdmin : adminClients.values()) {
            try {
                serverAdmin.updateMetadata(clusterState);
            } catch (ClientException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Active servers: " + adminClients.keySet());
    }

    @Override
    public void start() {
        for (KVAdminInterface connection : adminClients.values()) {
            try {
                connection.start();
            } catch (ClientException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void stop() {
        for (KVAdminInterface connection : adminClients.values()) {
            try {
                connection.stop();
            } catch (ClientException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void shutDown() {
        // TODO: send a shutdown request to all active instances via KVAdminInterface
        for (KVAdminInterface connection : adminClients.values()) {
            try {
                connection.shutDown();
            } catch (ClientException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void addNode(int cacheSize, CacheReplacementStrategy displacementStrategy) {
        // TODO: select a node randomly from the known but inactive servers - DONE
        // TODO: start a server instance on the selected node via SSH - DONE
        // TODO: use HashRing to get the successor - DONE
        // TODO: the value range (x,z) of the successor will change, determine the new value range of the successor (y,z) and of the new node (x,y) - DONE
        // TODO: enable the write lock on the SUCCESSOR node - DONE
        // TODO: initiate data transfer for the key range of the new node from the successor to the new node - DONE
        // TODO: after transfer completed, send updateMetadataRequest to ALL active nodes

        Set<InetSocketAddress> allNodes = servers.stream()
                .map(server -> server.address)
                .collect(Collectors.toSet());
        Set<InetSocketAddress> activeServers = adminClients.keySet();
        Set<InetSocketAddress> candidateServers = new HashSet<>(allNodes);
        candidateServers.removeAll(activeServers);

        System.out.println(candidateServers);
        String workingDir = System.getProperty("user.dir");

        // final InetSocketAddress currentNode;

        HashRing hashRing = new HashRing();
        adminClients.keySet().forEach(hashRing::addNode);

        candidateServers.stream().findFirst().ifPresent(candidateServer -> {
            System.out.println("Adding server: " + candidateServer);
            String command = String.format("bash nohup java -jar %s/ms3-server.jar %d %d %s &", workingDir, candidateServer.getPort(), cacheSize, displacementStrategy.name());
            System.out.println("command: " + command);

            // create client
            KVAdminInterface admin = new DummyAdminClient(candidateServer);
            adminClients.put(candidateServer, admin);

            hashRing.addNode(candidateServer);
            String remoteUser = servers.stream().filter(s -> s.address == candidateServer).findFirst().map(s -> s.userName).get();
            try {
                executeSSH(candidateServer, remoteUser, command);
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }

            InetSocketAddress successor = hashRing.getSuccessor(candidateServer);
            Range assignedRange = hashRing.getAssignedRange(candidateServer);
            lockWriteAndMoveData(successor, candidateServer, assignedRange);
        });

        List<NodeEntry> clusterState = new LinkedList<>();
        for (InetSocketAddress server: hashRing.getNodes()) {
            clusterState.add(new NodeEntry("testname", server, new Range(0,1)));
        }
        for (KVAdminInterface serverAdmin : adminClients.values()) {
            try {
                serverAdmin.updateMetadata(clusterState);
            } catch (ClientException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Active servers: " + adminClients.keySet());

    }

    private void lockWriteAndMoveData(InetSocketAddress source, InetSocketAddress destination, Range keyRange) {
        try {
            System.out.printf("Transferring data from %s to %s: %s\n", source, destination, keyRange);
            KVAdminInterface sourceAdmin = adminClients.get(source);
            sourceAdmin.enableWriteLock();

            sourceAdmin.moveData(destination, keyRange);

            MaintenanceStatusResponse status = sourceAdmin.getMaintenanceStatus();
            while (status.isActive()) {
                System.out.printf("Waiting for maintenance task %s on %s, progress %3d%%.\n", status.getTask(), source, status.getProgress());
                break; // TODO remove once we have real status
            }

            sourceAdmin.disableWriteLock();
        } catch (ClientException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void removeNode() {
        // TODO: randomly select one of the active nodes
        // TODO: recalculate the value range of the successor node by adding the range of the node to be removed
        // TODO: enable write lock on the server to be deleted
        // TODO: initiate data transfer for the key range of the server to be deleted to it's successor
        // TODO: after this is done, send a metadata update to ALL servers
        // TODO: shut down the node to be removed

        // TODO display the number of running servers

        Set<InetSocketAddress> allNodes = servers.stream()
                .map(server -> server.address)
                .collect(Collectors.toSet());
        Set<InetSocketAddress> activeServers = adminClients.keySet();

        HashRing hashRing = new HashRing();
        adminClients.keySet().forEach(hashRing::addNode);

        activeServers.stream().findAny().ifPresent(nodeToBeRemoved -> {
            System.out.println("Removing server: " + nodeToBeRemoved);
            InetSocketAddress successor = hashRing.getSuccessor(nodeToBeRemoved);
            Range assignedRange = hashRing.getAssignedRange(nodeToBeRemoved);
            hashRing.removeNode(nodeToBeRemoved);
            lockWriteAndMoveData(nodeToBeRemoved, successor, assignedRange);

            KVAdminInterface adminRemoved = adminClients.remove(nodeToBeRemoved);
            try {
                adminRemoved.shutDown();
            } catch (ClientException e) {
                e.printStackTrace();
            }
            adminRemoved.disconnect();
        });

        List<NodeEntry> clusterState = new LinkedList<>();
        for(InetSocketAddress server: hashRing.getNodes()) {
            clusterState.add(new NodeEntry("testname1", server, new Range(0,1)));
        }

        for (KVAdminInterface serverAdmin : adminClients.values()) {
            try {
                serverAdmin.updateMetadata(clusterState);
            } catch (ClientException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Active servers: " + adminClients.keySet());

    }

    private void executeSSH(InetSocketAddress remoteServer, String remoteUser, String command) throws IOException, InterruptedException {
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
        // TODO catch exceptions here and check the error code for success
    }

    public static void main(String[] args)  {
        DefaultKVAdmin kvadmin = new DefaultKVAdmin(Arrays.asList(
                new ServerInfo("node1", "xhens", new InetSocketAddress("localhost", 50000)),
                new ServerInfo("node2", "xhens", new InetSocketAddress("localhost", 50001)),
                new ServerInfo("node3", "xhens", new InetSocketAddress("localhost", 50002)),
                new ServerInfo("node4", "xhens", new InetSocketAddress("localhost", 50003)),
                new ServerInfo("node4", "xhens", new InetSocketAddress("localhost", 50004))
        ));
        kvadmin.initService(1, 45, CacheReplacementStrategy.LRU);
        kvadmin.addNode(25, CacheReplacementStrategy.FIFO);
        kvadmin.removeNode();
    }
}
