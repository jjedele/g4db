package common.messages.mapreduce;

/**
 * Requests the status of a map/reduce job.
 */
public class MRStatusRequest implements MRMessage {

    /** The type code for serialization. */
    public static final byte TYPE_CODE = 0x05;

    private final String id;

    /**
     * Constructor.
     *
     * @param id ID of the job.
     */
    public MRStatusRequest(String id) {
        this.id = id;
    }

    /**
     * Return the ID of the job.
     *
     * @return ID of the job.
     */
    public String getId() {
        return id;
    }

}
