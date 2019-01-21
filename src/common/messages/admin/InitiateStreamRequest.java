package common.messages.admin;

import common.hash.Range;
import common.messages.gossip.ClusterDigest;
import common.utils.HostAndPort;

/**
 * Initiates a data stream of a range of keys to a given destination.
 */
public class InitiateStreamRequest extends AdminMessage {

    /** The type code for serialization. */
    public static final byte TYPE_CODE = 0x0B;

    private final HostAndPort destination;
    private final Range keyRange;
    private final ClusterDigest clusterDigest;
    private final boolean moveReplicationTarget;

    /**
     * Constructor.
     * @param destination Recipient for the data
     * @param keyRange Range of keys that is streamed
     * @param clusterDigest Current knowledge of the cluster
     * @param moveReplicationTarget Make requester new replication target for moved data
     */
    public InitiateStreamRequest(HostAndPort destination,
                                 Range keyRange,
                                 ClusterDigest clusterDigest,
                                 boolean moveReplicationTarget) {
        this.destination = destination;
        this.keyRange = keyRange;
        this.clusterDigest = clusterDigest;
        this.moveReplicationTarget = moveReplicationTarget;
    }

    /**
     * Return the destination of this stream request.
     * @return
     */
    public HostAndPort getDestination() {
        return destination;
    }

    /**
     * Return the key range for this stream request.
     * @return
     */
    public Range getKeyRange() {
        return keyRange;
    }

    /**
     * Return the associated cluster state.
     * @return
     */
    public ClusterDigest getClusterDigest() {
        return clusterDigest;
    }

    /**
     * Return if the request should switch the target ranges replication target
     * @return
     */
    public boolean isMoveReplicationTarget() {
        return moveReplicationTarget;
    }
}
