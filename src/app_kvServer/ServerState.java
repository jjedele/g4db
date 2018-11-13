package app_kvServer;

import common.hash.NodeEntry;
import common.hash.Range;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Global server state.
 */
public class ServerState implements ServerStateMBean {

    private final String id;
    private final Properties properties;
    private volatile boolean stopped;
    private volatile boolean writeLockActive;
    private volatile Range keyRange;
    private volatile Set<NodeEntry> clusterNodes;

    /**
     * Constructor.
     * @param id Unique ID used for persisted parts of the state.
     */
    public ServerState(String id) {
        this.id = id;
        this.properties = new Properties();
        this.stopped = true;
        this.writeLockActive = false;
        this.keyRange = new Range();
        this.clusterNodes = new HashSet<>();
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
     * Get the key range this server is responsible for.
     * @return Key range
     */
    public synchronized Range getKeyRange() {
        return keyRange;
    }

    /**
     * Set the key range this server is responsible for.
     * @param keyRange Key range
     */
    public synchronized void setKeyRange(Range keyRange) {
        this.keyRange = keyRange;
        this.properties.setProperty("keyRange", String.format("%d:%d", keyRange.getStart(), keyRange.getEnd()));
    }

    /**
     * Return the current cluster nodes.
     * @return Set of cluster nodes
     */
    public synchronized Set<NodeEntry> getClusterNodes() {
        return clusterNodes;
    }

    /**
     * Set the current cluster nodes.
     * @param clusterNodes Set of current cluster nodes
     */
    public synchronized void setClusterNodes(Set<NodeEntry> clusterNodes) {
        this.clusterNodes = clusterNodes;
    }

}
