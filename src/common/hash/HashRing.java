package common.hash;

import java.net.InetSocketAddress;

/**
 * Consistent Hashing distributes a key range and nodes across a ring
 * such that each key belongs to one node and the assignments change
 * as little as possible when nodes are added or deleted.
 */
public class HashRing {

    /**
     * Add a node to the hash ring.
     * @param node Address of the node
     */
    public void addNode(InetSocketAddress node) {
        // TODO
    }

    /**
     * Remove a node from the hash ring.
     * @param node Address of the node
     */
    public void removeNode(InetSocketAddress node) {
        // TODO
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
     * Return the hash for given value.
     * @param val The value
     * @return The hash
     */
    public int getHash(String val) {
        // TODO
        return 0;
    }

}
