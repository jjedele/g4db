package app_kvServer.admin;

/**
 * Execution engine for maintenance tasks.
 */
public class AdminTasks {

    private static AdminTask currentTask;

    /**
     * Schedule a maintenance task for synchronization.
     * @param task The task
     */
    public static synchronized void addTask(AdminTask task) {
        if (currentTask != null && currentTask.getProgress() < 1) {
            // TODO at some point we might want to queue them
            throw new IllegalStateException("Can only have one active admin task at a time.");
        }

        currentTask = task;
        new Thread(currentTask).start();
    }

    /**
     * Return the name of the currently running task.
     * @return Name or null if no active task
     */
    public static synchronized String getTaskType() {
        if (hasActiveTask()) {
            return currentTask.getClass().getName();
        }
        return null;
    }

    /**
     * Return the progress of the current maintenance tasks.
     * @return The progress
     */
    public static synchronized float getProgress() {
        if (currentTask == null) {
            return 0;
        }

        return currentTask.getProgress();
    }

    /**
     * Return if there is an active maintenance task.
     * @return True if there is an active maintenance task
     */
    public static synchronized boolean hasActiveTask() {
        if (currentTask == null) {
            return false;
        }

        if (currentTask.getProgress() == 1.0) {
            return false;
        }

        return true;
    }

}
