package client.exceptions;

public class ConnectionException extends ClientException {

    public ConnectionException(String message) {
        super(message);
    }

    public ConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConnectionException(Throwable cause) {
        super(cause);
    }

    public ConnectionException(String host, int port) {
        super(String.format("Could not connect to %s:%d.", host, port));
    }

}
