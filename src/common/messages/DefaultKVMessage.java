package common.messages;

/**
 * Default key,value message.
 *
 * This is the principal data structure for this application.
 */
public class DefaultKVMessage implements KVMessage {

    private final String key;
    private final String value;
    private final StatusType status;

    /**
     * Default constructor.
     * @param key Key
     * @param value Value
     * @param status Status code
     */
    public DefaultKVMessage(String key, String value, StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getKey() {
        return key;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getValue() {
        return value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StatusType getStatus() {
        return status;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return String.format("<%s, %s, %d>", key, value, status);
    }

}
