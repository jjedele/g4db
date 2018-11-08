package common.messages.admin;

/**
 * A generic response that can be successful or unsuccessful and
 * possibly contains a status/error message.
 */
public class GenericResponse extends AdminMessage {

    private GenericResponse(boolean success, String message) {
        // TODO
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
        // TODO
        return false;
    }

    /**
     * Return the status/error message.
     * @return The message (can be null)
     */
    public String getMessage() {
        // TODO
        return null;
    }

}
