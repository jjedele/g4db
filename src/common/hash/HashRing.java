package common.hash;

import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Collections;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

/**
 * Consistent Hashing distributes a key range and nodes across a ring
 * such that each key belongs to one node and the assignments change
 * as little as possible when nodes are added or deleted.
 */
public class HashRing {
    private final TreeMap<Integer, InetSocketAddress> circle = new TreeMap<Integer, InetSocketAddress>();

    /**
     * Add a node to the hash ring.
     * @param node Address of the node
     */
    public void addNode(InetSocketAddress node) {
        int nodeKey = getHash(node.toString());
        //System.out.println(node.toString() + " md5:" + nodeKey);
        circle.put(nodeKey, node);
    }

    /**
     * Remove a node from the hash ring.
     * @param node Address of the node
     */
    public void removeNode(InetSocketAddress node) {
        int nodeKey = getHash(node.toString());
        circle.remove(nodeKey);
    }

    /**
     * Return all nodes on the ring.
     * @return Server nodes
     */
    public Collection<InetSocketAddress> getNodes() {
        // TODO
        return circle.values();
    }

    /**
     * Return the range of hash values a node is responsible for.
     * @param node Address of the node
     * @return {@link Range} of hash values
     * @throws IllegalArgumentException if the node does not exist
     */
    public Range getAssignedRange(InetSocketAddress node) {
        // TODO
        // do later
        return new Range(0, 1);
    }

    /**
     * Return the node responsible for the given value.
     * @param val The value
     * @return Address of the responsible node
     */
    public InetSocketAddress getResponsibleNode(String val) {
        // TODO
        int hashVal = getHash(val);
        for (int key : circle.navigableKeySet()) {
            if (hashVal <= key)
                return circle.get(key);
        }
        return circle.firstEntry().getValue();
        //test
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
        int nodeKey = getHash(node.toString());
        if (circle.higherKey(nodeKey) == null)
            return circle.firstEntry().getValue();
        return circle.higherEntry(nodeKey).getValue();

    }

    /**
     * Return the hash for given value.
     * @param val The value
     * @return The hash
     */
    public int getHash(String val) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] d = md.digest(val.getBytes());
            int hash = d[0] << 24 | (d[1] & 0xff) << 16 | (d[2] & 0xff) << 8 | (d[3] & 0xff);
            return hash;
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError("Won't happen.");
        }
    }

}
