package common.messages.admin;

/**
 * Request status about any running maintenance tasks from the server.
 */
public class GetMaintenanceStatusRequest extends AdminMessage {

    /** The type code for serialization. */
    public static final byte TYPE_CODE = 0x09;

}
