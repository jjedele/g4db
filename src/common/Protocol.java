package common;

import common.exceptions.ProtocolException;
import common.messages.KVMessage;

/**
 * Implementation of the wire protocol.
 */
public final class Protocol {

    private Protocol() {}

    /**
     * Encodes a {@link KVMessage} into binary format to transfer it over the network.
     * @param message message to marshal
     * @return encoded data as per protocol
     */
    public static byte[] encode(KVMessage message) {
        // TODO
        return null;
    }

    // TODO: decide if we want to do this
//    /**
//     * Encodes an {@link ProtocolException} into binary format to transfer it over the network.
//     * @param exception exception to marshal
//     * @return encoded data as per protocol
//     */
//    public static byte[] encode(ProtocolException exception) {
//        // TODO
//        return null;
//    }

    /**
     * Decodes binary data as per protocol.
     * @param payload the binary data
     * @return the decoded message
     * @throws ProtocolException if the data actually encoded an exception
     */
    public static KVMessage decode(byte[] payload) throws ProtocolException {
        // TODO
        return null;
    }

}
