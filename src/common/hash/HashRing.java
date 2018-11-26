package common.hash;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Consistent Hashing distributes a key range and nodes across a ring
 * such that each key belongs to one node and the assignments change
 * as little as possible when nodes are added or deleted.
 */
public class HashRing {
    private final TreeMap<Integer, InetSocketAddress> circle = new TreeMap<Integer, InetSocketAddress>();

    private final List<InetSocketAddress> nodes = new ArrayList<>();

    /**
     * Add a node to the hash ring.
     * @param node Address of the node
     */

    private String addressToString(InetSocketAddress node){
        return String.format("%s:%d", node.getHostString(), node.getPort());

    }

    public void addNode(InetSocketAddress node) {
        int nodeKey = getHash(addressToString(node));
        circle.put(nodeKey, node);
    }

    /**
     * Remove a node from the hash ring.
     * @param node Address of the node
     */
    public void removeNode(InetSocketAddress node) {
        int nodeKey = getHash(addressToString(node));
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
        int upperBound = getHash(addressToString(node));
        int lowerBound = Optional
                .ofNullable(circle.lowerKey(upperBound))
                .orElse(circle.lastKey());
        return new Range(lowerBound, upperBound);
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
        //In case the predecessor node has a higher position than the actual server
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
        int nodeKey = getHash(addressToString(node));
        if (circle.higherKey(nodeKey) == null)
            return circle.firstEntry().getValue();
        return circle.higherEntry(nodeKey).getValue();

    }

    /**
     * Return if the hash ring contains given node.
     * @param node Node
     * @return True if contained
     */
    public boolean contains(InetSocketAddress node) {
        return circle.containsValue(node);
    }

    /**
     * Return if the hash ring is empty.
     * @return True if empty
     */
    public boolean isEmpty() {
        return circle.isEmpty();
    }

    // dynamic wrapper to we can override it
    protected int getHash(String val) {
        return hash(val);
    }

    /**
     * Return the hash for given value.
     * @param val The value
     * @return The hash
     */
    public static int hash(String val) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] d = md.digest(val.getBytes());
            //Since it is easier to only look at the first 4 Byte
            int hash = d[0] << 24 | (d[1] & 0xff) << 16 | (d[2] & 0xff) << 8 | (d[3] & 0xff);
            return hash;
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError("Won't happen.");
        }
    }

}
