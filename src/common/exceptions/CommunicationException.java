package common.exceptions;

import java.io.IOException;

public class CommunicationException extends IOException {

    public CommunicationException(String message) {
        super(message);
    }

    public CommunicationException(String message, Throwable cause) {
        super(message, cause);
    }

}
