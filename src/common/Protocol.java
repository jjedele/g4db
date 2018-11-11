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
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Implementation of the wire protocol.
 */
public final class Protocol {

    private Protocol() {
    }

    private static final char UNIT_SEPARATOR = 0x1f;

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
        StringBuilder sb = new StringBuilder();

        // write correlation number
        sb.append(correlationNumber);
        sb.append(UNIT_SEPARATOR);

        if (msg instanceof KVMessage) {
            encodeKVMessage(sb, (KVMessage) msg);
        } else if (msg instanceof AdminMessage) {
            encodeAdminMessage(sb, (AdminMessage) msg);
        } else if (msg instanceof ExceptionMessage) {
            encodeExceptionMessage(sb, (ExceptionMessage) msg);
        } else {
            throw new AssertionError("Unsupported message class: " + msg.getClass());
        }

        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Decode an encoded {@link Message}.
     *
     * @param data Message in encoded binary format
     * @return {@link DecodingResult}
     * @throws ProtocolException if the binary data does not conform to the protocol
     */
    public static DecodingResult decode(byte[] data) throws ProtocolException {
        try {
            Scanner scanner = new Scanner(new String(data, StandardCharsets.UTF_8)).
                    useDelimiter(Character.toString(UNIT_SEPARATOR));

            // correlation number
            long correlationNumber = Long.parseLong(scanner.next());

            // content type
            byte contentType = Byte.parseByte(scanner.next());

            if (contentType == ContentType.KV_MESSAGE) {
                KVMessage kvMessage = decodeKVMessage(scanner);
                return new DecodingResult(correlationNumber, kvMessage);

            } else if (contentType == ContentType.EXCEPTION) {
                ExceptionMessage exceptionMessage = decodeExceptionMessage(scanner);
                return new DecodingResult(correlationNumber, exceptionMessage);

            } else if (contentType == ContentType.ADMIN) {
                AdminMessage adminMessage = decodeAdminMessage(scanner);
                return new DecodingResult(correlationNumber, adminMessage);

            } else {
                throw new ProtocolException("Unsupported content type: " + contentType);
            }
        } catch (RuntimeException e) {
            throw new ProtocolException("Message can not be decoded.", e);
        }
    }

    private static void encodeKVMessage(StringBuilder sb, KVMessage msg) {
        // content type
        sb.append(ContentType.KV_MESSAGE);
        sb.append(UNIT_SEPARATOR);

        // operation type
        sb.append(msg.getStatus().opCode);
        sb.append(UNIT_SEPARATOR);

        // key
        if (msg.getKey() != null) {
            // TODO: escape unit separator
            sb.append(msg.getKey());
        }
        sb.append(UNIT_SEPARATOR);

        // value
        if (msg.getValue() != null) {
            // TODO: escape unit separator
            sb.append(msg.getValue());
        }
        sb.append(UNIT_SEPARATOR);
    }

    private static void encodeExceptionMessage(StringBuilder sb, ExceptionMessage msg) {
        // content type
        sb.append(ContentType.EXCEPTION);
        sb.append(UNIT_SEPARATOR);

        // exception class
        sb.append(msg.getExceptionClass());
        sb.append(UNIT_SEPARATOR);

        // message
        sb.append(msg.getMessage());
        sb.append(UNIT_SEPARATOR);
    }

    private static void encodeAdminMessage(StringBuilder sb, AdminMessage msg) {
        // content type
        sb.append(ContentType.ADMIN);
        sb.append(UNIT_SEPARATOR);

        if (msg instanceof GenericResponse) {
            encodeGenericResponse(sb, (GenericResponse) msg);
        } else if (msg instanceof UpdateMetadataRequest) {
            encodeUpdateMetadataRequest(sb, (UpdateMetadataRequest) msg);
        } else {
            throw new AssertionError("Unsupported AdminMessage: " + msg.getClass());
        }
    }

    private static void encodeGenericResponse(StringBuilder sb, GenericResponse msg) {
        sb.append(GenericResponse.TYPE_CODE);
        sb.append(UNIT_SEPARATOR);

        sb.append(Boolean.toString(msg.isSuccess()));
        sb.append(UNIT_SEPARATOR);

        sb.append(msg.getMessage());
        sb.append(UNIT_SEPARATOR);
    }

    private static void encodeUpdateMetadataRequest(StringBuilder sb, UpdateMetadataRequest req) {
        sb.append(UpdateMetadataRequest.TYPE_CODE);
        sb.append(UNIT_SEPARATOR);

        for (NodeEntry node : req.getNodes()) {
            sb.append(node.name);
            sb.append(UNIT_SEPARATOR);

            sb.append(node.address.getHostName());
            sb.append(UNIT_SEPARATOR);

            sb.append(node.address.getPort());
            sb.append(UNIT_SEPARATOR);

            sb.append(Integer.toUnsignedString(node.keyRange.getStart()));
            sb.append(UNIT_SEPARATOR);

            sb.append(Integer.toUnsignedString(node.keyRange.getEnd()));
            sb.append(UNIT_SEPARATOR);
        }
    }

    private static KVMessage decodeKVMessage(Scanner scanner) {
        // operation type
        byte opCode = Byte.parseByte(scanner.next());
        KVMessage.StatusType operationType = STATUS_BY_OPCODE.get(opCode);

        // key
        String key = scanner.next();
        if (key.length() == 0) {
            key = null;
        }

        // value
        String value = scanner.next();
        if (value.length() == 0) {
            value = null;
        }

        return new DefaultKVMessage(key, value, operationType);
    }

    private static ExceptionMessage decodeExceptionMessage(Scanner scanner) {
        String className = scanner.next();
        String message = scanner.next();

        return new ExceptionMessage(className, message);
    }

    private static AdminMessage decodeAdminMessage(Scanner scanner) throws ProtocolException {
        byte type = Byte.parseByte(scanner.next());

        if (type == GenericResponse.TYPE_CODE) {
            boolean success = Boolean.parseBoolean(scanner.next());
            String msg = scanner.next();

            return new GenericResponse(success, msg);
        } else if (type == UpdateMetadataRequest.TYPE_CODE) {
            UpdateMetadataRequest updateMetadataRequest = new UpdateMetadataRequest();

            while (scanner.hasNext()) {
                String nodeName = scanner.next();
                String hostName = scanner.next();
                int port = Integer.parseInt(scanner.next());
                int rangeStart = Integer.parseUnsignedInt(scanner.next());
                int rangeEnd = Integer.parseUnsignedInt(scanner.next());

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
