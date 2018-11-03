package common.exceptions;

/**
 * A RemoteException represents a general exception state that happened on
 * the other end of communication.
 */
public class RemoteException extends ProtocolException {

    /**
     * {@inheritDoc}
     */
    public RemoteException(String message) {
        super(message);
    }

}
