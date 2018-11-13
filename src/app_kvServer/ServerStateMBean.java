package app_kvServer;

/**
 * Simple JMX-based administration interface.
 */
public interface ServerStateMBean {

    boolean isStopped();
    void setStopped(boolean stopped);
    boolean isWriteLockActive();
    void setWriteLockActive(boolean writeLockActive);

}
