package common.messages.admin;

/**
 * Request to disable the write lock on the server.
 */
public class DisableWriteLockRequest extends AdminMessage {

    /** The type code for serialization. */
    public static final byte TYPE_CODE = 0x07;

}