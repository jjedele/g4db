package common.hash;

import common.utils.HostAndPort;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An entry of a node responsibility table.
 */
public final class NodeEntry {

    /** Name of the node */
    public final String name;

    /** Address of the node */
    public final HostAndPort address;

    /** Key range the node is responsible for */
    public final Range keyRange;

    /**
     * Default constructor.
     * @param name Name of the node
     * @param address Address of the node
     * @param keyRange Key range the node is responsible for
     */
    public NodeEntry(String name, HostAndPort address, Range keyRange) {
        this.name = name;
        this.address = address;
        this.keyRange = keyRange;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return String.format("Node(%s, %s, %s)", name, address, keyRange);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return address.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof NodeEntry) {
            NodeEntry other = (NodeEntry) obj;
            return this.name.equals(other.name)
                    && this.address.equals(other.address)
                    && this.keyRange.equals(other.keyRange);
        } else {
            return false;
        }
    }

    /**
     * Converts this NodeEntry into a machine parsable string representation.
     * @return Encoded string
     */
    public String toSerializableString() {
        return String.format("%s:%s:%d:%d:%d",
                name, address.getHost(), address.getPort(), keyRange.getStart(), keyRange.getEnd());
    }

    /**
     * Converts a string-encoded representation to a NodeEntry instance.
     * @param s Encoded string
     * @return NodeEntry
     */
    public static NodeEntry fromSerializableString(String s) {
        String[] parts = s.split(":");
        if (parts.length != 5) {
            throw new IllegalArgumentException("Not a validly encoded NodeEntry: " + s);
        }

        String nodeName = parts[0];
        String hostString = parts[1];

        int port;
        int rangeStart;
        int rangeEnd;
        try {
            port = Integer.parseInt(parts[2]);
            rangeStart = Integer.parseInt(parts[3]);
            rangeEnd = Integer.parseInt(parts[4]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Not a validly encoded NodeEntry: " + s);
        }
        return new NodeEntry(
                nodeName, new HostAndPort(hostString, port), new Range(rangeStart, rangeEnd));
    }

    /**
     * Converts a collection of NodeEntries into a machine parsable string representation.
     * @param nodes Nodes
     * @return Encoded string
     */
    public static String multipleToSerializableString(Collection<NodeEntry> nodes) {
        return nodes.stream()
                .map(NodeEntry::toSerializableString)
                .collect(Collectors.joining(","));
    }

    /**
     * Converts a string-encoded version of multiple nodes back into a Java collection.
     * @param s Encoded string
     * @return Node entries
     */
    public static List<NodeEntry> mutlipleFromSerializedString(String s) {
        return Stream.of(s.split(","))
                .map(NodeEntry::fromSerializableString)
                .collect(Collectors.toList());
    }

}