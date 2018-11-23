package app_kvServer;

/**
 * A SessionRegistry keeps track of all open sessions.
 */
public interface SessionRegistry {

    /**
     * Add active session to the registry.
     * @param session The session
     */
    void registerSession(ClientConnection session);

    /**
     * Remove a session from the registry.
     * @param session The session
     */
    void unregisterSession(ClientConnection session);

    /**
     * Request a global server shutdown.
     * TODO: this does not really belong in here - factor out
     */
    void requestShutDown();

}
