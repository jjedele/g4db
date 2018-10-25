package app_kvServer.persistence;

/**
 * An error condition from the persistence layer.
 */
public class PersistenceException extends Exception {

    /**
     * {@inheritDoc}
     */
    public PersistenceException(String message) {
        super(message);
    }

    /**
     * {@inheritDoc}
     */
    public PersistenceException(String message, Throwable cause) {
        super(message, cause);
    }

}
