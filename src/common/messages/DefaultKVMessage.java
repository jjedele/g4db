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

    //GET
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

    //PUT
    /**
     * {@inheritDoc}
     */
    @Override
    public String putKey() {
        return key;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String putValue() {
        return value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StatusType putStatus() {
        return status;
    }


    //PUT_SUCCESS
    @Override
    public String putSuccessKey() {
        return key;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String putSuccessValue() {
        return value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StatusType putSuccessStatus() {
        return status;
    }


    //PUT_ERROR
    @Override
    public String putErrorKey() {
        return key;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String putErrorValue() {
        return value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StatusType putErrorStatus() {
        return status;
    }



    //PUT_UPDATE
    /**
     * {@inheritDoc}
     */
    @Override
    public String putUpdateKey() {
        return key;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public String putUpdateValue() {
        return value;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public StatusType putUpdateStatus() {
        return status;
    }

    //GET_ERROR
    /**
     * {@inheritDoc}
     */
    @Override
    public String getErrorKey() {
        return key;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public String getErrorValue() {
        return value;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public StatusType getErrorStatus() {
        return status;
    }


    //GET_SUCCESS
    /**
     * {@inheritDoc}
     */
    @Override
    public String getSuccessKey() {
        return key;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public String getSuccessValue() {
        return value;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public StatusType getSuccessStatus() {
        return status;
    }


    //DELETE
    /**
     * {@inheritDoc}
     */
    @Override
    public String deleteKey() {
        return key;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public String deleteValue() {
        return value;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public StatusType deleteStatus() {
        return status;
    }



    //DELETE_SUCCESS
    /**
     * {@inheritDoc}
     */
    @Override
    public String deleteSuccessKey() {
        return key;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public String deleteSuccessValue() {
        return value;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public StatusType deleteSuccessStatus() {
        return status;
    }


    //DELETE_ERROR
    /**
     * {@inheritDoc}
     */
    @Override
    public String deleteErrorKey() {
        return key;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public String deleteErrorValue() {
        return value;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public StatusType deleteErrorStatus() {
        return status;
    }


}