package common;

import common.exceptions.ProtocolException;
import common.messages.DefaultKVMessage;
import common.messages.KVMessage;
import common.utils.RecordReader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of the wire protocol.
 */
public final class Protocol {

    private Protocol() {
    }

    private static final byte UNIT_SEPARATOR = 0x1f;

    private static final Map<Byte, KVMessage.StatusType> STATUS_BY_OPCODE;
    static {
        Map<Byte, KVMessage.StatusType> map = new HashMap<>();
        for (KVMessage.StatusType status : KVMessage.StatusType.values()) {
            map.put(status.opCode, status);
        }
        STATUS_BY_OPCODE = Collections.unmodifiableMap(map);
    }

    /**
     * Encodes a {@link KVMessage} into binary format to transfer it over the network.
     *
     * @param message message to marshal
     * @return encoded data as per protocol
     */
    public static byte[] encode(KVMessage message) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        // add content type, static for now
        bos.write(ContentType.KV_MESSAGE);

        // status type / op code
        bos.write(message.getStatus().opCode);

        // TODO check for which messages key, value actually may be null

        // key
        String key = message.getKey();
        if (key != null) {
            byte[] keyData = key.getBytes(StandardCharsets.UTF_8);
            bos.write(keyData, 0, keyData.length);
        }
        bos.write(UNIT_SEPARATOR);

        // value
        String value = message.getValue();
        if (value != null) {
            byte[] valueData = value.getBytes(StandardCharsets.UTF_8);
            bos.write(valueData, 0, valueData.length);
        }
        bos.write(UNIT_SEPARATOR);

        return bos.toByteArray();
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
            if (payload.length < 4) {
                throw new ProtocolException("Too short to be a valid message");
            }

            byte contentType = payload[0];

            if (contentType != ContentType.KV_MESSAGE) {
                throw new ProtocolException("Unsupported content type: " + contentType);
            }

            byte statusCode = payload[1];
            KVMessage.StatusType status = STATUS_BY_OPCODE.get(statusCode);

            if (status == null) {
                throw new ProtocolException("Unknown op code: " + statusCode);
            }

            byte data[] = new byte[payload.length - 2];
            System.arraycopy(payload, 2, data, 0, data.length);

            try {
                RecordReader reader = new RecordReader(data, UNIT_SEPARATOR);

                byte[] keyData = reader.read();
                byte[] valueData = reader.read();

                String key = null;
                if (keyData != null && keyData.length > 0) {
                    key = new String(keyData, StandardCharsets.UTF_8);
                }

                String value = null;
                if (valueData != null && valueData.length > 0) {
                    value = new String(valueData, StandardCharsets.UTF_8);
                }

                // TODO check when key, value actually may be null

                return new DefaultKVMessage(key, value, status);
            } catch (IOException e) {
                throw new ProtocolException("Error decoding message.", e);
            }
        }


}
