package common.messages.admin;

import common.messages.gossip.ClusterDigest;

/**
 * Response for a requested key range stream.
 */
public class InitiateStreamResponse extends AdminMessage {

    /** The type code for serialization. */
    public static final byte TYPE_CODE = 0x0C;

    private final boolean success;
    private final String streamId;
    private final int numberOfItems;
    private final ClusterDigest clusterDigest;

    /**
     * Constructor.
     * @param success True if the nodes agree on the cluster state and the transfer will happen, false if there is a
     *                disagreement regarding the cluster state. In that case, an updated cluster digest will be
     *                included in this message.
     * @param streamId ID of the stream if it takes place
     * @param numberOfItems Number of items that will be streamed
     * @param clusterDigest Cluster state of the responding node in case it disagrees
     */
    public InitiateStreamResponse(boolean success, String streamId, int numberOfItems, ClusterDigest clusterDigest) {
        this.success = success;
        this.streamId = streamId;
        this.numberOfItems = numberOfItems;
        this.clusterDigest = clusterDigest;
    }

    /**
     * Return if the stream request was successful
     * @return
     */
    public boolean isSuccess() {
        return success;
    }

    /**
     * Return the ID of the stream.
     * @return
     */
    public String getStreamId() {
        return streamId;
    }

    /**
     * Return the number of items that will be streamed.
     * @return
     */
    public int getNumberOfItems() {
        return numberOfItems;
    }

    /**
     * Return the responding node's view of the cluster.
     * @return Cluster digest or null if there is no disagreement
     */
    public ClusterDigest getClusterDigest() {
        return clusterDigest;
    }

}
