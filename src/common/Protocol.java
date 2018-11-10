package common;

import common.exceptions.ProtocolException;
import common.hash.NodeEntry;
import common.hash.Range;
import common.messages.DefaultKVMessage;
import common.messages.ExceptionMessage;
import common.messages.KVMessage;
import common.messages.Message;
import common.messages.admin.AdminMessage;
import common.messages.admin.GenericResponse;
import common.messages.admin.UpdateMetadataRequest;
import common.utils.BinaryUtils;
import common.utils.ByteArrayReader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
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
     * Encode a {@link Message} for network transport.
     *
     * @param msg The message
     * @param correlationNumber A sequence number to correlate the message with
     * @return Binary encoded message
     */
    public static byte[] encode(Message msg, long correlationNumber) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        try {
            // write correlation number
            BinaryUtils.writeLong(bos, correlationNumber);

            if (msg instanceof KVMessage) {
                encodeKVMessage(bos, (KVMessage) msg);
            } else if (msg instanceof AdminMessage) {
                encodeAdminMessage(bos, (AdminMessage) msg);
            } else if (msg instanceof ExceptionMessage) {
                encodeExceptionMessage(bos, (ExceptionMessage) msg);
            } else {
                throw new AssertionError("Unsupported message class: " + msg.getClass());
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not encode message.", e);
        }

        return bos.toByteArray();
    }

    /**
     * Decode an encoded {@link Message}.
     *
     * @param data Message in encoded binary format
     * @return {@link DecodingResult}
     * @throws ProtocolException if the binary data does not conform to the protocol
     */
    public static DecodingResult decode(byte[] data) throws ProtocolException {
        ByteArrayReader reader = new ByteArrayReader(data);

        // correlation number
        long correlationNumber = reader.readLong();

        // content type
        byte contentType = reader.readByte();

        if (contentType == ContentType.KV_MESSAGE) {
            KVMessage kvMessage = decodeKVMessage(reader);
            return new DecodingResult(correlationNumber, kvMessage);

        } else if (contentType == ContentType.EXCEPTION) {
            ExceptionMessage exceptionMessage = decodeExceptionMessage(reader);
            return new DecodingResult(correlationNumber, exceptionMessage);

        } else if (contentType == ContentType.ADMIN) {
            AdminMessage adminMessage = decodeAdminMessage(reader);
            return new DecodingResult(correlationNumber, adminMessage);

        } else {
            throw new ProtocolException("Unsupported content type: " + contentType);
        }
    }

    private static void encodeKVMessage(OutputStream os, KVMessage msg) throws IOException {
        // content type
        os.write(ContentType.KV_MESSAGE);

        // operation type
        os.write(msg.getStatus().opCode);

        // key
        if (msg.getKey() != null) {
            BinaryUtils.writeString(os, msg.getKey());
        }
        os.write(UNIT_SEPARATOR);

        // value
        if (msg.getValue() != null) {
            BinaryUtils.writeString(os, msg.getValue());
        }
    }

    private static void encodeExceptionMessage(OutputStream os, ExceptionMessage msg) throws IOException {
        // content type
        os.write(ContentType.EXCEPTION);

        // exception class
        BinaryUtils.writeString(os, msg.getExceptionClass());
        os.write(UNIT_SEPARATOR);

        // message
        BinaryUtils.writeString(os, msg.getMessage());
    }

    private static void encodeAdminMessage(OutputStream os, AdminMessage msg) throws IOException {
        // content type
        os.write(ContentType.ADMIN);

        if (msg instanceof GenericResponse) {
            encodeGenericResponse(os, (GenericResponse) msg);
        } else if (msg instanceof UpdateMetadataRequest) {
            encodeUpdateMetadataRequest(os, (UpdateMetadataRequest) msg);
        } else {
            throw new AssertionError("Unsupported AdminMessage: " + msg.getClass());
        }
    }

    private static void encodeGenericResponse(OutputStream os, GenericResponse msg) throws IOException {
        os.write(GenericResponse.TYPE_CODE);
        BinaryUtils.writeBool(os, msg.isSuccess());
        BinaryUtils.writeString(os, msg.getMessage());
    }

    private static void encodeUpdateMetadataRequest(OutputStream os, UpdateMetadataRequest req) throws IOException {
        os.write(UpdateMetadataRequest.TYPE_CODE);
        for (NodeEntry node : req.getNodes()) {
            BinaryUtils.writeString(os, node.name);
            os.write(UNIT_SEPARATOR);
            BinaryUtils.writeString(os, node.address.getHostName());
            os.write(UNIT_SEPARATOR);
            BinaryUtils.writeInt(os, node.address.getPort());
            BinaryUtils.writeInt(os, node.keyRange.getStart());
            BinaryUtils.writeInt(os, node.keyRange.getEnd());
        }
    }

    private static KVMessage decodeKVMessage(ByteArrayReader reader) {
        // operation type
        byte opCode = reader.readByte();
        KVMessage.StatusType operationType = STATUS_BY_OPCODE.get(opCode);

        // key
        String key = null;
        byte[] keyData = reader.readUntil(UNIT_SEPARATOR);
        if (keyData.length > 0) {
            key = new String(keyData, StandardCharsets.UTF_8);
        }

        // value
        String value = null;
        byte[] valueData = reader.readRest();
        if (valueData.length > 0) {
            value = new String(valueData, StandardCharsets.UTF_8);
        }

        return new DefaultKVMessage(key, value, operationType);
    }

    private static ExceptionMessage decodeExceptionMessage(ByteArrayReader reader) {
        String className = new String(reader.readUntil(UNIT_SEPARATOR));
        String message = new String(reader.readRest(), StandardCharsets.UTF_8);

        return new ExceptionMessage(className, message);
    }

    private static AdminMessage decodeAdminMessage(ByteArrayReader reader) throws ProtocolException {
        byte type = reader.readByte();

        if (type == GenericResponse.TYPE_CODE) {
            boolean success = reader.readBoolean();
            String msg = new String(reader.readRest(), StandardCharsets.UTF_8);

            return new GenericResponse(success, msg);
        } else if (type == UpdateMetadataRequest.TYPE_CODE) {
            UpdateMetadataRequest updateMetadataRequest = new UpdateMetadataRequest();

            while (reader.hasRemainingData()) {
                String nodeName = new String(reader.readUntil(UNIT_SEPARATOR));
                String hostName = new String(reader.readUntil(UNIT_SEPARATOR));
                int port = reader.readInt();
                int rangeStart = reader.readInt();
                int rangeEnd = reader.readInt();

                updateMetadataRequest.addNode(new NodeEntry(
                        nodeName,
                        new InetSocketAddress(hostName, port),
                        new Range(rangeStart, rangeEnd)));
            }

            return updateMetadataRequest;
        } else {
            throw new ProtocolException("Unknown admin message type: " + type);
        }
    }

}
