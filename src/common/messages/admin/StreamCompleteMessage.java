package common.messages.admin;

import common.hash.Range;

/**
 * Send to a node to notify him that a requested key range was transferred completely.
 */
public class StreamCompleteMessage extends AdminMessage {

    /** The type code for serialization. */
    public static final byte TYPE_CODE = 0x0D;

    private final String streamId;
    private final Range range;

    /**
     * Constructor.
     * @param streamId ID of the completed stream
     * @param range Range of keys that has been transferred
     */
    public StreamCompleteMessage(String streamId, Range range) {
        this.streamId = streamId;
        this.range = range;
    }

    /**
     * Return the ID of the completed stream.
     * @return
     */
    public String getStreamId() {
        return streamId;
    }

    /**
     * Return the transferred key range.
     * @return
     */
    public Range getRange() {
        return range;
    }

}
