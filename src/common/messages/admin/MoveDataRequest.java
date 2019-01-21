package common.messages.admin;

import common.hash.Range;
import common.utils.HostAndPort;

/**
 * Request the server the move data within a given key range to another server.
 */
public class MoveDataRequest extends AdminMessage {

    /** The type code for serialization. */
    public static final byte TYPE_CODE = 0x08;

    private final HostAndPort destination;
    private final Range range;

    /**
     * Constructor.
     * @param destination Address of the server to move the data to
     * @param range Range of keys to transfer
     */
    public MoveDataRequest(HostAndPort destination, Range range) {
        this.destination = destination;
        this.range = range;
    }

    /**
     * Return the address of the destination server.
     * @return Address
     */
    public HostAndPort getDestination() {
        return destination;
    }

    /**
     * Return the key range to be transferred.
     * @return Key range
     */
    public Range getRange() {
        return range;
    }

}
