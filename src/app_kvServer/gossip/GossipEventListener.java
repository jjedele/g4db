package app_kvServer.gossip;

import common.messages.gossip.ClusterDigest;
import common.messages.gossip.ServerState;

import java.net.InetSocketAddress;

/**
 * Implemented by classes that need to listen for cluster changes.
 */
public interface GossipEventListener {

    /**
     * Called after the cluster changed.
     * @param clusterDigest
     */
    default void clusterChanged(ClusterDigest clusterDigest) {}

    /**
     * Called when a new node is detected.
     * @param node The new node
     * @param newState State of the added node
     */
    default void nodeAdded(InetSocketAddress node, ServerState.Status newState) {}

    /**
     * Called when a state change for a node is detected.
     * @param node The node that changed
     * @param newState The new state
     */
    default void nodeChanged(InetSocketAddress node, ServerState.Status newState) {}

}
