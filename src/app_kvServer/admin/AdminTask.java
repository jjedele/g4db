package app_kvServer.admin;


/**
 * A maintenance task for the cluster.
 */
public interface AdminTask extends Runnable {

    float getProgress();

}
