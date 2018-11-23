package app_kvEcs;

import app_kvServer.CacheReplacementStrategy;
import com.jcraft.jsch.JSchException;

/**
 * Administration interface for a cluster of KVServers.
 */
public interface KVAdmin {

    /**
     * Initializes the cluster.
     *
     * Randomly choose <numberOfNodes> servers from the available machines and start
     * the KVServer by issuing an SSH call to the respective machine. This call launches
     * the server with the specified cache size and displacement strategy.
     * All servers are initialized with the meta-data and remain in state stopped.
     *
     * @param numberOfNodes Number of nodes to start
     * @param cacheSize Size of the cache to configure on each node
     * @param displacementStrategy Cache strategy
     */
    void initService(int numberOfNodes, int cacheSize, CacheReplacementStrategy displacementStrategy);

    /**
     * Starts the storage service.
     *
     * This starts all server instances that participate in the cluster.
     */
    void start() throws JSchException;

    /**
     * Stops the storage service.
     *
     * All participating server instances are stopped for processing client requests
     * but the processes remain running.
     */
    void stop();

    /**
     * Stops all server instances and exits the remote processes.
     */
    void shutDown();

    /**
     * Add a server to the storage service.
     *
     * Create a new server instance with the specified cache size and displacement strategy
     * and add it to the storage service at an arbitrary position. This will trigger a transfer
     * of data from one of the servers to the new one.
     *
     * @param cacheSize Size of the cache
     * @param displacementStrategy Displacement strategy for the cache
     */
    void addNode(int cacheSize, CacheReplacementStrategy displacementStrategy);

    /**
     * Remove a server from the storage service.
     *
     * Disables and removes a server instance from the storage service at a random position.
     * This will trigger a transfer of data from the stopped server to another server
     * in the cluster.
     */
    void removeNode();

}
