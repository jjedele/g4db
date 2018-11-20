package common.messages.admin;

/**
 * Server reply containing status information about potentially running maintenance tasks.
 */
public class MaintenanceStatusResponse extends AdminMessage {

    /** The type code for serialization. */
    public static final byte TYPE_CODE = 0x0A;

    private final boolean active;
    private final String task;
    private final int progress;

    /**
     * Constructor.
     * @param task Name of the task that is currently running
     * @param progress Progress of the task. Range 0 - 100
     */
    public MaintenanceStatusResponse(boolean active, String task, int progress) {
        this.active = active;
        this.task = task;
        this.progress = progress;
    }

    /**
     * Return if there is currently an active admin task.
     * @return True if there is an active task
     */
    public boolean isActive() {
        return active;
    }

    /**
     * Return the name of the currently running task.
     * @return Name of the task or null if there is non
     */
    public String getTask() {
        return task;
    }

    /**
     * Return the progress of the running task.
     * @return Progress in percentage done, ranges from 0 - 100
     */
    public int getProgress() {
        return progress;
    }

}
