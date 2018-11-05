package common;

import common.exceptions.ProtocolException;
import common.exceptions.RemoteException;
import common.messages.DefaultKVMessage;
import common.messages.KVMessage;
import common.utils.RecordReader;
import org.apache.logging.log4j.ThreadContext;

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
        return encode(message, new CorrelationInformation(-1, -1));
    }

    /**
     * Encodes a {@link KVMessage} into binary format to transfer it over the network.
     *
     * @param message message to marshal
     * @param correlationInformation correlation information
     * @return encoded data as per protocol
     */
    public static byte[] encode(KVMessage message,
                                CorrelationInformation correlationInformation) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        writeCorrelationInformation(bos, correlationInformation);

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

    /**
     * Encodes an {@link Exception} into binary format to transfer it over the network.
     *
     * Only the message will be transferred.
     *
     * @param exception exception to marshal
     * @return encoded data as per protocol
     */
    public static byte[] encode(Exception exception) {
        return encode(exception, new CorrelationInformation(-1, -1));
    }

    /**
     * Encodes an {@link Exception} into binary format to transfer it over the network.
     *
     * Only the message will be transferred.
     *
     * @param exception exception to marshal
     * @param correlationInformation correlation information
     * @return encoded data as per protocol
     */
    public static byte[] encode(Exception exception,
                                CorrelationInformation correlationInformation) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        writeCorrelationInformation(bos, correlationInformation);

        // add content type, static for now
        bos.write(ContentType.EXCEPTION);

        // message
        String message = exception.getMessage();
        if (message !=  null) {
            byte[] messageData = message.getBytes(StandardCharsets.UTF_8);
            bos.write(messageData, 0, messageData.length);
        }
        bos.write(UNIT_SEPARATOR);

        return bos.toByteArray();
    }

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

        // TODO refactor this
        byte[] clientIDData = new byte[4];
        System.arraycopy(payload,0, clientIDData, 0, 4);
        int clientID = bytesToInt(clientIDData);

        byte[] correlationIDData = new byte[4];
        System.arraycopy(payload,4, correlationIDData, 0, 4);
        int correlationID = bytesToInt(correlationIDData);

        // put stuff into log4j directly for now
        ThreadContext.put("clientID", Integer.toString(clientID));
        ThreadContext.put("correlationID", Integer.toString(correlationID));

        byte contentType = payload[8];

        // TODO refactor this
        if (contentType == ContentType.EXCEPTION) {
            byte[] data = new byte[payload.length - 10];
            System.arraycopy(payload, 9, data, 0, data.length);
            String exceptionMsg = new String(data, StandardCharsets.UTF_8);
            throw new RemoteException(exceptionMsg);
        }

        if (contentType != ContentType.KV_MESSAGE) {
            throw new ProtocolException("Unsupported content type: " + contentType);
        }

        byte statusCode = payload[9];
        KVMessage.StatusType status = STATUS_BY_OPCODE.get(statusCode);

        if (status == null) {
            throw new ProtocolException("Unknown op code: " + statusCode);
        }

        byte data[] = new byte[payload.length - 10];
        System.arraycopy(payload, 10, data, 0, data.length);

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

    private static void writeCorrelationInformation(ByteArrayOutputStream bos,
                                                    CorrelationInformation correlationInformation) {
        // client ID
        byte[] clientIDData = intToBytes(correlationInformation.getClientId());
        bos.write(clientIDData, 0, clientIDData.length);

        // correlation ID
        byte[] correlationIDData = intToBytes(correlationInformation.getCorrelationId());
        bos.write(correlationIDData, 0, correlationIDData.length);
    }

    // Yannick Rochon, https://stackoverflow.com/questions/5399798/byte-array-and-int-conversion-in-java/5399829#5399829
    public static byte[] intToBytes(int n) {
        return new byte[] {
                (byte) ((n >> 24) & 0xFF),
                (byte) ((n >> 16) & 0xFF),
                (byte) ((n >> 8) & 0xFF),
                (byte) (n & 0xFF)
        };
    }

    // Yannick Rochon, https://stackoverflow.com/questions/5399798/byte-array-and-int-conversion-in-java/5399829#5399829
    public static int bytesToInt(byte[] b) {
        return   b[3] & 0xFF |
                (b[2] & 0xFF) << 8 |
                (b[1] & 0xFF) << 16 |
                (b[0] & 0xFF) << 24;
    }

}
