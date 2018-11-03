package common.messages;

public interface KVMessage {

    enum StatusType {
        GET((byte) 0x0),            /* Get - request */  //$ finish testing
        GET_ERROR((byte) 0x1),        /* requested tuple (i.e. value) not found */  //$ finish testing
        GET_SUCCESS((byte) 0x2),    /* requested tuple (i.e. value) found */  //$ finish testing
        PUT((byte) 0x3),            /* Put - request */  //$ finish testing
        PUT_SUCCESS((byte) 0x4),    /* Put - request successful, tuple inserted */  //$ finish testing
        PUT_UPDATE((byte) 0x5),    /* Put - request successful, i.e. value updated */ //$ finish testing
        PUT_ERROR((byte) 0x6),        /* Put - request not successful */  //$ finish testing
        DELETE((byte) 0x7),        /* Delete - request */  //$ finish testing
        DELETE_SUCCESS((byte) 0x8), /* Delete - request successful */ //$ finish testing
        DELETE_ERROR((byte) 0x9);    /* Delete - request successful */  //$ finish testing

        public final byte opCode;

        StatusType(byte opCode) {
            this.opCode = opCode;
        }
    }

    /**
     * @return the key that is associated with this message,
     * null if not key is associated.
     */
    public String getKey();

    /**
     * @return the value that is associated with this message,
     * null if not value is associated.
     */
    public String getValue();

    /**
     * @return a status string that is used to identify request types,
     * response types and error types associated to the message.
     */
    public StatusType getStatus();
}