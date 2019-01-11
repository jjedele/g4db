package common.messages.mapreduce;

/**
 * Result of initiating a map/reduce job.
 */
public class InitiateMRResponse implements MRMessage {

    /** The type code for serialization. */
    public static final byte TYPE_CODE = 0x02;

    private final String id;
    private final String error;

    /**
     * Constructor.
     * @param id ID of the job that was started.
     * @param error Possible error, might be null.
     */
    public InitiateMRResponse(String id, String error) {
        this.id = id;
        this.error = error;
    }

    /**
     * Return a stringified error or null if successful.
     * @return
     */
    public String getError() {
        return error;
    }

    /**
     * Return the ID of the job.
     * @return
     */
    public String getId() {
        return id;
    }

}
