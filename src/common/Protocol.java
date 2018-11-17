package common;

import common.exceptions.ProtocolException;
import common.hash.NodeEntry;
import common.messages.DefaultKVMessage;
import common.messages.ExceptionMessage;
import common.messages.KVMessage;
import common.messages.Message;
import common.messages.admin.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
     * @return {@link CorrelatedMessage}
     * @throws ProtocolException if the binary data does not conform to the protocol
     */
    public static CorrelatedMessage decode(byte[] data) throws ProtocolException {
        try {
            Scanner scanner = new Scanner(new String(data, StandardCharsets.UTF_8)).
                    useDelimiter(Character.toString(UNIT_SEPARATOR));

            // correlation number
            long correlationNumber = Long.parseLong(scanner.next());

            // content type
            byte contentType = Byte.parseByte(scanner.next());

            if (contentType == ContentType.KV_MESSAGE) {
                KVMessage kvMessage = decodeKVMessage(scanner);
                return new CorrelatedMessage(correlationNumber, kvMessage);

            } else if (contentType == ContentType.EXCEPTION) {
                ExceptionMessage exceptionMessage = decodeExceptionMessage(scanner);
                return new CorrelatedMessage(correlationNumber, exceptionMessage);

            } else if (contentType == ContentType.ADMIN) {
                AdminMessage adminMessage = decodeAdminMessage(scanner);
                return new CorrelatedMessage(correlationNumber, adminMessage);

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
        } else if (msg instanceof StartServerRequest) {
            encodeSimpleAdminMessage(sb, StartServerRequest.TYPE_CODE);
        } else if (msg instanceof StopServerRequest) {
            encodeSimpleAdminMessage(sb, StopServerRequest.TYPE_CODE);
        } else if (msg instanceof ShutDownServerRequest) {
            encodeSimpleAdminMessage(sb, ShutDownServerRequest.TYPE_CODE);
        } else if (msg instanceof EnableWriteLockRequest) {
            encodeSimpleAdminMessage(sb, EnableWriteLockRequest.TYPE_CODE);
        } else if (msg instanceof DisableWriteLockRequest) {
            encodeSimpleAdminMessage(sb, DisableWriteLockRequest.TYPE_CODE);
        } else {
            throw new AssertionError("Unsupported AdminMessage: " + msg.getClass());
        }
    }

    private static void encodeSimpleAdminMessage(StringBuilder sb, byte typeCode) {
        sb.append(typeCode);
        sb.append(UNIT_SEPARATOR);
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

        sb.append(encodeMultipleAddresses(req.getNodes()));
        sb.append(UNIT_SEPARATOR);
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

            String encodedNodeEntries = scanner.next();
            for (InetSocketAddress entry : decodeMultipleAddresses(encodedNodeEntries)) {
                updateMetadataRequest.addNode(entry);
            }

            return updateMetadataRequest;
        } else if (type == StartServerRequest.TYPE_CODE) {
            return new StartServerRequest();
        } else if (type == StopServerRequest.TYPE_CODE) {
            return new StopServerRequest();
        } else if (type == ShutDownServerRequest.TYPE_CODE) {
            return new ShutDownServerRequest();
        } else if (type == EnableWriteLockRequest.TYPE_CODE) {
            return new EnableWriteLockRequest();
        } else if (type == DisableWriteLockRequest.TYPE_CODE) {
            return new DisableWriteLockRequest();
        } else {
            throw new ProtocolException("Unknown admin message type: " + type);
        }
    }

    /**
     * Encode a socket address into string format.
     * @param address Address
     * @return Encoded address
     */
    public static String encodeAddress(InetSocketAddress address) {
        return String.format("%s:%d", address.getHostString(), address.getPort());
    }

    /**
     * Decode a socket address from string format.
     * @param encodedAddress Encoded address
     * @return Address
     */
    public static InetSocketAddress decodeAddress(String encodedAddress) {
        String[] parts = encodedAddress.split(":");
        return new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
    }

    /**
     * Encode multiple addresses into string format.
     * @param addresses Collection of addresses
     * @return Encoded string
     */
    public static String encodeMultipleAddresses(Collection<InetSocketAddress> addresses) {
        return addresses.stream()
                .map(Protocol::encodeAddress)
                .collect(Collectors.joining(","));
    }

    /**
     * Decode multiple address from string format.
     * @param s Encoded addresses
     * @return Decoded addresses
     */
    public static Collection<InetSocketAddress> decodeMultipleAddresses(String s) {
        return Stream.of(s.split(","))
                .map(Protocol::decodeAddress)
                .collect(Collectors.toList());
    }

}
