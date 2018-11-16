package app_kvEcs;

import app_kvServer.CacheReplacementStrategy;
import client.DummyAdminClient;
import client.KVAdminInterface;
import client.exceptions.ClientException;
import com.jcraft.jsch.JSchException;
import common.hash.HashRing;
import common.hash.NodeEntry;
import common.hash.Range;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

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
        // TODO: maintain list of possible server nodes
        this.servers = new ArrayList<>(servers);
        this.adminClients = new HashMap<>();
    }

    @Override
    public void initService(int numberOfNodes, int cacheSize, CacheReplacementStrategy displacementStrategy) {
        // TODO: randomly select <numberOfNodes> nodes from the available list

        // TODO: for each selected node, start an instance of KVServer via SSH (jsch library is in libs)
        // TODO: for each started node, create an instance of KVAdminInterface and connect it

        // TODO: use consistent hashing (HashRing class) to determine the value ranges the servers are responsible for
        // TODO: assemble responsibility/metadata table and send updateMetadata requests to all started servers via KVAdminInterface
        // TODO: maintain information about which nodes have active instances and also the HashRing instance

        // start servers
        String workingDir = System.getProperty("user.dir");
        HashRing hashRing = new HashRing();
        for (int i = 0; i < numberOfNodes; i++) {
            ServerInfo currentServer = servers.get(i);
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
    }

    @Override
    public void start() {
        // TODO: send a start request to all active instances via KVAdminInterface
    }

    @Override
    public void stop() {
        // TODO: send a stop request to all active instances via KVAdminInterface
    }

    @Override
    public void shutDown() {
        // TODO: send a shutdown request to all active instances via KVAdminInterface
    }

    @Override
    public void addNode(int cacheSize, CacheReplacementStrategy displacementStrategy) {
        // TODO: select a node randomly from the known but inactive servers
        // TODO: start a server instance on the selected node via SSH
        // TODO: use HashRing to get the successor
        // TODO: the value range (x,z) of the successor will change, determine the new value range of the successor (y,z) and of the new node (x,y)
        // TODO: enable the write lock on the SUCCESSOR node
        // TODO: initiate data transfer for the key range of the new node from the successor to the new node
        // TODO: after transfer completed, send updateMetadataRequest to ALL active nodes
    }

    @Override
    public void removeNode() {
        // TODO: randomly select one of the active nodes
        // TODO: recalculate the value range of the successor node by adding the range of the node to be removed
        // TODO: enable write lock on the server to be deleted
        // TODO: initiate data transfer for the key range of the server to be deleted to it's successor
        // TODO: after this is done, send a metadata update to ALL servers
        // TODO: shut down the node to be removed
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

    public static void main(String[] args) throws JSchException {
        DefaultKVAdmin kvadmin = new DefaultKVAdmin(Arrays.asList(
                new ServerInfo("node1", "xhens", new InetSocketAddress("localhost", 50000)),
                new ServerInfo("node2", "xhens", new InetSocketAddress("localhost", 50001)),
                new ServerInfo("node3", "xhens", new InetSocketAddress("localhost", 50002))
        ));
        kvadmin.initService(2, 42, CacheReplacementStrategy.FIFO);
    }
}
