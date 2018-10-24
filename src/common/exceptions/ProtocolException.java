package common.exceptions;

public class ProtocolException extends Exception {

    /**
     * {@inheritDoc}
     */
    public ProtocolException(String message) {
        super(message);
    }

    /**
     * {@inheritDoc}
     */
    public ProtocolException(String message, Throwable cause) {
        super(message, cause);
    }

}
