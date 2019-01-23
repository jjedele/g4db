package common.messages.mapreduce;

/**
 * Requests the status of a map/reduce job.
 */
public class MRStatusRequest implements MRMessage {

    /** The type code for serialization. */
    public static final byte TYPE_CODE = 0x05;

    public enum Type {
        MASTER,
        WORKER
    }

    private final String id;
    private final Type type;

    /**
     * Constructor.
     *
     * @param id ID of the job.
     * @param type
     */
    public MRStatusRequest(String id, Type type) {
        this.id = id;
        this.type = type;
    }

    /**
     * Return the ID of the job.
     *
     * @return ID of the job.
     */
    public String getId() {
        return id;
    }

    /**
     * Return which type of status is requested.
     *
     * @return
     */
    public Type getType() {
        return type;
    }
}
