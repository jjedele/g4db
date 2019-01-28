package common.messages.mapreduce;

/**
 * Status of a map/reduce job.
 */
public class MRStatusMessage implements MRMessage {

    /** The type code for serialization. */
    public static final byte TYPE_CODE = 0x06;

    public enum Status {
        RUNNING,
        FINISHED,
        FAILED,
        NOT_FOUND
    }

    private final String id;
    private final Status status;
    private final int workersTotal;
    private final int workersComplete;
    private final int workersFailed;
    private final int percentageComplete;
    private final String error;

    /**
     * Constructor.
     *  @param id Job ID.
     * @param status Running status.
     * @param workersTotal Number of workers participating in total.
     * @param workersComplete Number of completed workers.
     * @param workersFailed Number of workers that failed.
     * @param percentageComplete Percentage completed of the job.
     * @param error Error message, may be null.
     */
    public MRStatusMessage(String id, Status status, int workersTotal, int workersComplete,
                           int workersFailed, int percentageComplete, String error) {
        this.id = id;
        this.status = status;
        this.workersTotal = workersTotal;
        this.workersComplete = workersComplete;
        this.workersFailed = workersFailed;
        this.percentageComplete = percentageComplete;
        this.error = error;
    }

    /**
     * Return the status of the job.
     * @return
     */
    public Status getStatus() {
        return status;
    }

    /**
     * Return the ID of the job.
     * @return
     */
    public String getId() {
        return id;
    }

    /**
     * Return the number of workers participating.
     * @return
     */
    public int getWorkersTotal() {
        return workersTotal;
    }

    /**
     * Return the number of workers that completed.
     * @return
     */
    public int getWorkersComplete() {
        return workersComplete;
    }

    /**
     * Return the number of workers that failed.
     * @return
     */
    public int getWorkersFailed() {
        return workersFailed;
    }

    /**
     * Return the overall percentage completed.
     * @return
     */
    public int getPercentageComplete() {
        return percentageComplete;
    }

    /**
     * Return the error message or null if no error.
     * @return
     */
    public String getError() {
        return error;
    }

    @Override
    public String toString() {
        return String.format("<%s (%s); Workers: %d/%d (failed: %d); %d%%; error: %s>",
                id, status, workersComplete, workersTotal, workersFailed, percentageComplete, error);
    }
}
