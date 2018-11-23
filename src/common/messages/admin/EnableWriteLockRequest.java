package common.messages.admin;

/**
 * Request to enable the write lock on the server.
 */
public class EnableWriteLockRequest extends AdminMessage {

    /** The type code for serialization. */
    public static final byte TYPE_CODE = 0x06;

}