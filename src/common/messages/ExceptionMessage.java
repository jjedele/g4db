package common.messages;

/**
 * A general exception send over the network.
 */
public class ExceptionMessage implements Message {

    private final String exceptionClass;
    private final String message;

    /**
     * Constructor.
     * @param exceptionClass Name of the original exception class
     * @param message Message of the exception
     */
    public ExceptionMessage(String exceptionClass, String message) {
        this.exceptionClass = exceptionClass;
        this.message = message;
    }

    /**
     * Constructor.
     * @param subject The exception to encode with this message
     */
    public ExceptionMessage(Exception subject) {
        this.exceptionClass = subject.getClass().getName();
        this.message = subject.getMessage();
    }

    /**
     * Return the class of the original exception.
     * @return Qualified class name
     */
    public String getExceptionClass() {
        return exceptionClass;
    }

    /**
     * Return the message of the original exception.
     * @return Message
     */
    public String getMessage() {
        return message;
    }

}
