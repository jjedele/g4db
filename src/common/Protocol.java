package common;

import common.exceptions.ProtocolException;
import common.hash.Range;
import common.messages.DefaultKVMessage;
import common.messages.ExceptionMessage;
import common.messages.KVMessage;
import common.messages.Message;
import common.messages.admin.*;
import common.messages.gossip.ServerState;
import common.messages.gossip.ClusterDigest;
import common.messages.gossip.GossipMessage;

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
    public static final byte RECORD_SEPARATOR = 0x1e;
    public static final char GROUP_SEPARATOR = 0x1d;
    public static final byte[] SHUTDOWN_CMD = new byte[] {0x0, 0x42, 0x0};

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
        } else if (msg instanceof GossipMessage) {
            encodeGossipMessage(sb, (GossipMessage) msg);
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

            } else if (contentType == ContentType.GOSSIP) {
                GossipMessage gossipMessage = decodeGossipMessage(scanner);
                return new CorrelatedMessage(correlationNumber, gossipMessage);

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
            encodeStartServerRequest(sb, (StartServerRequest) msg);
        } else if (msg instanceof StopServerRequest) {
            encodeSimpleAdminMessage(sb, StopServerRequest.TYPE_CODE);
        } else if (msg instanceof ShutDownServerRequest) {
            encodeSimpleAdminMessage(sb, ShutDownServerRequest.TYPE_CODE);
        } else if (msg instanceof EnableWriteLockRequest) {
            encodeSimpleAdminMessage(sb, EnableWriteLockRequest.TYPE_CODE);
        } else if (msg instanceof DisableWriteLockRequest) {
            encodeSimpleAdminMessage(sb, DisableWriteLockRequest.TYPE_CODE);
        } else if (msg instanceof MoveDataRequest) {
            encodeMoveDataRequest(sb, (MoveDataRequest) msg);
        } else if (msg instanceof GetMaintenanceStatusRequest) {
            encodeSimpleAdminMessage(sb, GetMaintenanceStatusRequest.TYPE_CODE);
        } else if (msg instanceof MaintenanceStatusResponse) {
            encodeMaintenanceStatusResponse(sb, (MaintenanceStatusResponse) msg);
        } else if (msg instanceof InitiateStreamRequest) {
            encodeInitiateStreamRequest(sb, (InitiateStreamRequest) msg);
        } else if (msg instanceof InitiateStreamResponse) {
            encodeInitiateStreamResponse(sb, (InitiateStreamResponse) msg);
        } else if (msg instanceof StreamCompleteMessage) {
            encodeStreamCompleteMessage(sb, (StreamCompleteMessage) msg);
        } else {
            throw new AssertionError("Unsupported AdminMessage: " + msg.getClass());
        }
    }

    private static void encodeGossipMessage(StringBuilder sb, GossipMessage msg) {
        sb.append(ContentType.GOSSIP);
        sb.append(UNIT_SEPARATOR);

        // only cluster digests for now
        ClusterDigest digest = (ClusterDigest) msg;
        sb.append(encodeClusterDigest(digest));
        sb.append(UNIT_SEPARATOR);
    }

    private static void encodeSimpleAdminMessage(StringBuilder sb, byte typeCode) {
        sb.append(typeCode);
        sb.append(UNIT_SEPARATOR);
    }

    private static void encodeStartServerRequest(StringBuilder sb, StartServerRequest req) {
        sb.append(StartServerRequest.TYPE_CODE);
        sb.append(UNIT_SEPARATOR);

        sb.append(Boolean.toString(req.isClusterInit()));
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

    private static void encodeMoveDataRequest(StringBuilder sb, MoveDataRequest req) {
        sb.append(MoveDataRequest.TYPE_CODE);
        sb.append(UNIT_SEPARATOR);

        sb.append(encodeAddress(req.getDestination()));
        sb.append(UNIT_SEPARATOR);

        sb.append(req.getRange().getStart());
        sb.append(UNIT_SEPARATOR);

        sb.append(req.getRange().getEnd());
        sb.append(UNIT_SEPARATOR);
    }

    private static void encodeMaintenanceStatusResponse(StringBuilder sb, MaintenanceStatusResponse resp) {
        sb.append(MaintenanceStatusResponse.TYPE_CODE);
        sb.append(UNIT_SEPARATOR);

        sb.append(Boolean.toString(resp.isActive()));
        sb.append(UNIT_SEPARATOR);

        sb.append(Integer.toString(resp.getProgress()));
        sb.append(UNIT_SEPARATOR);

        sb.append(resp.getTask());
        sb.append(UNIT_SEPARATOR);
    }

    private static void encodeInitiateStreamRequest(StringBuilder sb, InitiateStreamRequest req) {
        sb.append(InitiateStreamRequest.TYPE_CODE);
        sb.append(UNIT_SEPARATOR);

        sb.append(encodeAddress(req.getDestination()));
        sb.append(UNIT_SEPARATOR);

        sb.append(encodeRange(req.getKeyRange()));
        sb.append(UNIT_SEPARATOR);

        sb.append(Boolean.toString(req.isMoveReplicationTarget()));
        sb.append(UNIT_SEPARATOR);

        sb.append(encodeClusterDigest(req.getClusterDigest()));
        sb.append(UNIT_SEPARATOR);
    }

    private static void encodeInitiateStreamResponse(StringBuilder sb, InitiateStreamResponse resp) {
        sb.append(InitiateStreamResponse.TYPE_CODE);
        sb.append(UNIT_SEPARATOR);

        sb.append(Boolean.toString(resp.isSuccess()));
        sb.append(UNIT_SEPARATOR);

        sb.append(resp.getStreamId());
        sb.append(UNIT_SEPARATOR);

        sb.append(Integer.toString(resp.getNumberOfItems()));
        sb.append(UNIT_SEPARATOR);

        if (resp.getClusterDigest() != null) {
            sb.append(encodeClusterDigest(resp.getClusterDigest()));
            sb.append(UNIT_SEPARATOR);
        }
    }

    private static void encodeStreamCompleteMessage(StringBuilder sb, StreamCompleteMessage msg) {
        sb.append(StreamCompleteMessage.TYPE_CODE);
        sb.append(UNIT_SEPARATOR);

        sb.append(msg.getStreamId());
        sb.append(UNIT_SEPARATOR);

        sb.append(encodeRange(msg.getRange()));
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
            boolean clusterInit = Boolean.parseBoolean(scanner.next());

            return new StartServerRequest(clusterInit);
        } else if (type == StopServerRequest.TYPE_CODE) {
            return new StopServerRequest();
        } else if (type == ShutDownServerRequest.TYPE_CODE) {
            return new ShutDownServerRequest();
        } else if (type == EnableWriteLockRequest.TYPE_CODE) {
            return new EnableWriteLockRequest();
        } else if (type == DisableWriteLockRequest.TYPE_CODE) {
            return new DisableWriteLockRequest();
        } else if (type == MoveDataRequest.TYPE_CODE) {
            InetSocketAddress address = decodeAddress(scanner.next());
            int rangeStart = Integer.parseInt(scanner.next());
            int rangeEnd = Integer.parseInt(scanner.next());

            return new MoveDataRequest(address, new Range(rangeStart, rangeEnd));
        } else if (type == GetMaintenanceStatusRequest.TYPE_CODE) {
            return new GetMaintenanceStatusRequest();
        } else if (type == MaintenanceStatusResponse.TYPE_CODE) {
            boolean active = Boolean.parseBoolean(scanner.next());
            int progress = Integer.parseInt(scanner.next());
            String task = scanner.next();

            return new MaintenanceStatusResponse(active, task, progress);
        } else if (type == InitiateStreamRequest.TYPE_CODE) {
            InetSocketAddress target = decodeAddress(scanner.next());
            Range range = decodeRange(scanner.next());
            boolean isSwitchReplicationTarget = Boolean.parseBoolean(scanner.next());
            ClusterDigest digest = decodeClusterDigest(scanner.next());

            return new InitiateStreamRequest(target, range, digest, isSwitchReplicationTarget);
        } else if (type == InitiateStreamResponse.TYPE_CODE) {
            boolean success = Boolean.parseBoolean(scanner.next());
            String streamId = scanner.next();
            int numberOfItems = Integer.parseInt(scanner.next());
            ClusterDigest clusterDigest = null;
            if (scanner.hasNext()) {
                clusterDigest = decodeClusterDigest(scanner.next());
            }

            return new InitiateStreamResponse(success, streamId, numberOfItems, clusterDigest);
        } else if (type == StreamCompleteMessage.TYPE_CODE) {
            String streamId = scanner.next();
            Range range = decodeRange(scanner.next());

            return new StreamCompleteMessage(streamId, range);
        } else {
            throw new ProtocolException("Unknown admin message type: " + type);
        }
    }

    private static GossipMessage decodeGossipMessage(Scanner scanner) {
        // only ClusterDigests for now
        String encoded = scanner.next();

        return decodeClusterDigest(encoded);
    }

    private static String encodeRange(Range range) {
        return String.format("%d:%d", range.getStart(), range.getEnd());
    }

    private static Range decodeRange(String encoded) {
        String[] parts = encoded.split(":");
        int start = Integer.parseInt(parts[0]);
        int end = Integer.parseInt(parts[1]);
        return new Range(start, end);
    }

    private static String encodeClusterDigest(ClusterDigest digest) {
        StringJoiner digestJoiner = new StringJoiner("\n");
        for (Map.Entry<InetSocketAddress, ServerState> e : digest.getCluster().entrySet()) {
            StringJoiner recordJoiner = new StringJoiner(":");
            recordJoiner.add(e.getKey().getHostString());
            recordJoiner.add(Integer.toString(e.getKey().getPort()));
            recordJoiner.add(Long.toString(e.getValue().getGeneration()));
            recordJoiner.add(Long.toString(e.getValue().getHeartBeat()));
            recordJoiner.add(e.getValue().getStatus().name());
            recordJoiner.add(Long.toString(e.getValue().getStateVersion()));

            digestJoiner.add(recordJoiner.toString());
        }
        return digestJoiner.toString();
    }

    private static ClusterDigest decodeClusterDigest(String encoded) {
        Map<InetSocketAddress, ServerState> cluster = new HashMap<>();

        if (encoded != null && encoded.length() > 0) {
            Arrays.stream(encoded.split("\n")).forEach(nodeStr -> {
                String[] parts = nodeStr.split(":");
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                long generation = Long.parseLong(parts[2]);
                long heartBeat = Long.parseLong(parts[3]);
                ServerState.Status state = ServerState.Status.valueOf(parts[4]);
                long stateVersion = Long.parseLong(parts[5]);

                cluster.put(new InetSocketAddress(host, port), new ServerState(generation, heartBeat, state, stateVersion));
            });
        }

        return new ClusterDigest(cluster);
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
