package common.hash;

import java.net.InetSocketAddress;

/**
 * An entry of a node responsibility table.
 */
public final class NodeEntry {

    /** Name of the node */
    public final String name;

    /** Address of the node */
    public final InetSocketAddress address;

    /** Key range the node is responsible for */
    public final Range keyRange;

    /**
     * Default constructor.
     * @param name Name of the node
     * @param address Address of the node
     * @param keyRange Key range the node is responsible for
     */
    public NodeEntry(String name, InetSocketAddress address, Range keyRange) {
        this.name = name;
        this.address = address;
        this.keyRange = keyRange;
    }

}