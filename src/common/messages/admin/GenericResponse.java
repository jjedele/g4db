package common.messages.admin;

/**
 * A generic response that can be successful or unsuccessful and
 * possibly contains a status/error message.
 */
public class GenericResponse extends AdminMessage {

    /** The type code for serialization. */
    public static final byte TYPE_CODE = 0x01;

    private final boolean success;
    private final String message;

    /**
     * Default constructor.
     * @param success If the request was successful
     * @param message Status/error message
     */
    public GenericResponse(boolean success, String message) {
        this.success = success;
        this.message = message;
    }

    /**
     * Create a success response.
     * @param message An optional status message
     * @return The response
     */
    public static GenericResponse success(String message) {
        return new GenericResponse(true, message);
    }

    /**
     * Create an error response.
     * @param message An optional error message.
     * @return The response
     */
    public static GenericResponse error(String message) {
        return new GenericResponse(false, message);
    }

    /**
     * Return if the response represents a success
     * @return True if success
     */
    public boolean isSuccess() {
        return success;
    }

    /**
     * Return the status/error message.
     * @return The message (can be null)
     */
    public String getMessage() {
        return message;
    }

}
