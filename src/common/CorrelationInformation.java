package common;

/**
 * Information necessary to track messages across node boundaries.
 */
public class CorrelationInformation {

    private final int clientId;
    private final int correlationId;

    /**
     * Default constructor.
     * @param clientId ID of the client
     * @param correlationId ID of the message
     */
    public CorrelationInformation(int clientId, int correlationId) {
        this.clientId = clientId;
        this.correlationId = correlationId;
    }

    /**
     * Return the ID of the client
     * @return client ID
     */
    public int getClientId() {
        return clientId;
    }

    /**
     * Return the ID of the message
     * @return correlation ID
     */
    public int getCorrelationId() {
        return correlationId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return String.format("<COR %d,%d>", clientId, correlationId);
    }

}
