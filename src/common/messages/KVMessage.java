package common.messages;

public interface KVMessage {

    enum StatusType {
        GET((byte) 0x0), 			/* Get - request */
        GET_ERROR((byte) 0x1), 		/* requested tuple (i.e. value) not found */
        GET_SUCCESS((byte) 0x2), 	/* requested tuple (i.e. value) found */
        PUT((byte) 0x3), 			/* Put - request */
        PUT_SUCCESS((byte) 0x4), 	/* Put - request successful, tuple inserted */
        PUT_UPDATE((byte) 0x5), 	/* Put - request successful, i.e. value updated */
        PUT_ERROR((byte) 0x6), 		/* Put - request not successful */
        DELETE((byte) 0x7), 		/* Delete - request */
        DELETE_SUCCESS((byte) 0x8), /* Delete - request successful */
        DELETE_ERROR((byte) 0x9); 	/* Delete - request successful */

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


