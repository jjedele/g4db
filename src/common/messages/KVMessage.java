package common.messages;

public interface KVMessage extends Message {

    /**
     * Defines the typ of data encoded in this message.
     */
    enum StatusType {
        /** Request a value. */
        GET(0x0),
        /** Requested value was not found */
        GET_ERROR(0x1),
        /** Reply with requested value. */
        GET_SUCCESS(0x2),

        /** Request to store a value. */
        PUT(0x3),
        /** Value was successfully stored. */
        PUT_SUCCESS(0x4),
        /** An existing value with the given key has been successfully updated. */
        PUT_UPDATE(0x5),
        /** Value could not be stored. */
        PUT_ERROR(0x6),

        /** Request to delete a value. */
        DELETE(0x7),
        /** Value was successfully deleted. */
        DELETE_SUCCESS(0x8),
        /** Value could not be deleted. */
        DELETE_ERROR(0x9),

        /** Server is not serving client requests at this time. */
        SERVER_STOPPED(0xA),
        /** Server does not allow write access at this time because a data transfer is in progress. */
        SERVER_WRITE_LOCK(0xB),
        /** Server is not responsible for the provided key. */
        SERVER_NOT_RESPONSIBLE(0xC),

        /** Request to store a value for a replica */
        PUT_REPLICA(0xD),
        /** Request to delete a value for a replica */
        DELETE_REPLICA(0xE);

        public final byte opCode;

        StatusType(int opCode) {
            this.opCode = (byte) opCode;
        }
    }

    /**
     * @return the key that is associated with this message,
     * null if not key is associated.
     */
    String getKey();

    /**
     * @return the value that is associated with this message,
     * null if not value is associated.
     */
    String getValue();

    /**
     * @return a status string that is used to identify request types,
     * response types and error types associated to the message.
     */
    StatusType getStatus();

}