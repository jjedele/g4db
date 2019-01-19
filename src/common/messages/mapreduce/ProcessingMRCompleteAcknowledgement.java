package common.messages.mapreduce;

/**
 * ACK reply for {@link ProcessingMRCompleteMessage}.
 */
public class ProcessingMRCompleteAcknowledgement implements MRMessage {

    /** The type code for serialization. */
    public static final byte TYPE_CODE = 0x04;

}
