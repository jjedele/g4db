package common.messages.admin;

import common.hash.Range;
import common.messages.gossip.ClusterDigest;

import java.net.InetSocketAddress;

/**
 * Initiates a data stream of a range of keys to a given destination.
 */
public class InitiateStreamRequest extends AdminMessage {

    /** The type code for serialization. */
    public static final byte TYPE_CODE = 0x0B;

    private final InetSocketAddress destination;
    private final Range keyRange;
    private final ClusterDigest clusterDigest;

    /**
     * Constructor.
     * @param destination Recipient for the data
     * @param keyRange Range of keys that is streamed
     * @param clusterDigest Current knowledge of the cluster
     */
    public InitiateStreamRequest(InetSocketAddress destination, Range keyRange, ClusterDigest clusterDigest) {
        this.destination = destination;
        this.keyRange = keyRange;
        this.clusterDigest = clusterDigest;
    }

    /**
     * Return the destination of this stream request.
     * @return
     */
    public InetSocketAddress getDestination() {
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

}
