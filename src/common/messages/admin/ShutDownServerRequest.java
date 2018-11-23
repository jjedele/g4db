package common.messages.admin;

/**
 * Request to shutdown the server.
 */
public class ShutDownServerRequest extends AdminMessage {

    /** The type code for serialization. */
    public static final byte TYPE_CODE = 0x05;

}