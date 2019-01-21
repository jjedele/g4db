package app_kvServer;

import common.Protocol;
import common.hash.HashRing;
import common.utils.HostAndPort;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * Global server state.
 */
public class ServerState implements ServerStateMBean {

    private final static Logger LOG = LogManager.getLogger(ServerState.class);

    private final HostAndPort myself;
    private volatile boolean stopped;
    private volatile boolean writeLockActive;
    private volatile HashRing hashRing;

    /**
     * Constructor.
     * @param myself Address of the currently running server
     */
    public ServerState(HostAndPort myself) {
        this.myself = myself;
        this.stopped = true;
        this.writeLockActive = false;
        this.hashRing = new HashRing();
        hashRing.addNode(myself);
    }

    /**
     * Return the address of the current server.
     * @return Address
     */
    public HostAndPort getMyself() {
        return myself;
    }

    /**
     * Return if the server is currently stopped.
     * @return Running state
     */
    public synchronized boolean isStopped() {
        return stopped;
    }

    /**
     * Set the running state of the server.
     * @param stopped Running state
     */
    public synchronized void setStopped(boolean stopped) {
        this.stopped = stopped;
    }

    /**
     * Return if the server is currently locked for writes.
     * @return Write lock status
     */
    public synchronized boolean isWriteLockActive() {
        return writeLockActive;
    }

    /**
     * Set the write lock status for the server.
     * @param writeLockActive Write lock status
     */
    public synchronized void setWriteLockActive(boolean writeLockActive) {
        this.writeLockActive = writeLockActive;
    }

    /**
     * Return the current cluster nodes.
     * @return Set of cluster nodes
     */
    public synchronized HashRing getClusterNodes() {
        return hashRing;
    }

    /**
     * Set the current cluster nodes.
     * @param clusterNodes Set of current cluster nodes
     */
    public synchronized void setClusterNodes(Collection<HostAndPort> clusterNodes) {
        this.hashRing = new HashRing();

        for (HostAndPort nodeEntry : clusterNodes) {
            hashRing.addNode(nodeEntry);
        }

        LOG.info(hashRing);
    }

    //
    @Override
    public void setClusterNodesFromString(String s) {
        setClusterNodes(Protocol.decodeMultipleAddresses(s));
    }

}
