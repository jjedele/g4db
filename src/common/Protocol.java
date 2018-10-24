package common;

import common.exceptions.ProtocolException;
import common.messages.DefaultKVMessage;
import common.messages.KVMessage;
import common.utils.RecordReader;

import java.io.IOException;

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
        // TODO replace with real implementation
        String combined = "UNSUPPORTED OPERATION\n";
        if (message.getStatus() == KVMessage.StatusType.GET_SUCCESS) {
            combined = String.format("GET_SUCCESS: %s -> %s\n", message.getKey(), message.getValue());
        } else if (message.getStatus() == KVMessage.StatusType.PUT_SUCCESS) {
            combined = String.format("PUT_SUCCESS: %s -> %s\n", message.getKey(), message.getValue());
        }
        return combined.getBytes();
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
        // TODO replace with real implementation
        try {
            RecordReader fieldReader = new RecordReader(payload, (byte) ' ');

            String operationCode = new String(fieldReader.read());

            if ("PUT".equals(operationCode)) {
                String key = new String(fieldReader.read());
                String value = new String(fieldReader.read());
                return new DefaultKVMessage(key, value, KVMessage.StatusType.PUT);
            } else if ("GET".equals(operationCode)) {
                String key = new String(fieldReader.read());
                return new DefaultKVMessage(key, null, KVMessage.StatusType.GET);
            } else {
                throw new ProtocolException("Unknown op code: " + operationCode);
            }
        } catch (NullPointerException | IOException e) {
            throw new ProtocolException("Could not decode message");
        }
    }

}
