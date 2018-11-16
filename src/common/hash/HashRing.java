package common.hash;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Consistent Hashing distributes a key range and nodes across a ring
 * such that each key belongs to one node and the assignments change
 * as little as possible when nodes are added or deleted.
 */
public class HashRing {

    private final List<InetSocketAddress> nodes = new ArrayList<>();

    /**
     * Add a node to the hash ring.
     * @param node Address of the node
     */
    public void addNode(InetSocketAddress node) {
        // TODO
        nodes.add(node);
    }

    /**
     * Remove a node from the hash ring.
     * @param node Address of the node
     */
    public void removeNode(InetSocketAddress node) {
        // TODO
    }

    /**
     * Return all nodes on the ring.
     * @return Server nodes
     */
    public Collection<InetSocketAddress> getNodes() {
        // TODO
        return nodes;
    }

    /**
     * Return the range of hash values a node is responsible for.
     * @param node Address of the node
     * @return {@link Range} of hash values
     * @throws IllegalArgumentException if the node does not exist
     */
    public Range getAssignedRange(InetSocketAddress node) {
        // TODO
        return new Range(0, 1);
    }

    /**
     * Return the node responsible for the given value.
     * @param val The value
     * @return Address of the responsible node
     */
    public InetSocketAddress getResponsibleNode(String val) {
        // TODO
        return null;
    }

    /**
     * Return the node after the given one if it will be added.
     *
     * This is the node who's value range will change.
     *
     * @param node The node that potentially will be added
     * @return Successor node on the ring
     */
    public InetSocketAddress getSuccessor(InetSocketAddress node) {
        // TODO
        return null;
    }

    /**
     * Return the hash for given value.
     * @param val The value
     * @return The hash
     */
    public int getHash(String val) {
        // TODO
        return 0;
    }

}
