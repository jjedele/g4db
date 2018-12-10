package common.messages.admin;

/**
 * Request to start the server.
 */
public class StartServerRequest extends AdminMessage {

    /** The type code for serialization. */
    public static final byte TYPE_CODE = 0x03;

    private final boolean clusterInit;

    public StartServerRequest(boolean clusterInit) {
        this.clusterInit = clusterInit;
    }

    public boolean isClusterInit() {
        return clusterInit;
    }

}
