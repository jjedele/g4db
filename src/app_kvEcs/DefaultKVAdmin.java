package app_kvEcs;

import app_kvServer.CacheReplacementStrategy;

import java.net.InetSocketAddress;
import java.util.Collection;

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

        /** Address of the server */
        public final InetSocketAddress address;

        /**
         * Default constructor.
         * @param name Name of the server
         * @param address Address of the server
         */
        public ServerInfo(String name, InetSocketAddress address) {
            this.name = name;
            this.address = address;
        }

    }

    public DefaultKVAdmin(Collection<ServerInfo> servers) {
        // TODO: maintain list of possible server nodes
    }

    @Override
    public void initService(int numberOfNodes, int cacheSize, CacheReplacementStrategy displacementStrategy) {
        // TODO: randomly select <numberOfNodes> nodes from the available list
        // TODO: for each selected node, start an instance of KVServer via SSH (jsch library is in libs)
        // TODO: for each started node, create an instance of KVAdminInterface and connect it
        // TODO: use consistent hashing (HashRing class) to determine the value ranges the servers are responsible for
        // TODO: assemble responsibility/metadata table and send updateMetadata requests to all started servers via KVAdminInterface
        // TODO: maintain information about which nodes have active instances and also the HashRing instance
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
}
