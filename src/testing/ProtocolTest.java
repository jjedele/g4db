package testing;

import common.CorrelatedMessage;
import common.Protocol;
import common.exceptions.ProtocolException;
import common.hash.Range;
import common.messages.DefaultKVMessage;
import common.messages.ExceptionMessage;
import common.messages.KVMessage;
import common.messages.admin.*;
import common.messages.gossip.ServerState;
import common.messages.gossip.ClusterDigest;
import common.messages.mapreduce.InitiateMRRequest;
import common.messages.mapreduce.InitiateMRResponse;
import junit.framework.TestCase;

import java.net.InetSocketAddress;
import java.util.*;

public class ProtocolTest extends TestCase {

    public void testEncodeDecodeKVMessages() throws ProtocolException {
        KVMessage[] messages = new KVMessage[] {
                new DefaultKVMessage("foo", "bar", KVMessage.StatusType.PUT),
                new DefaultKVMessage("foo", null, KVMessage.StatusType.PUT_SUCCESS),
                new DefaultKVMessage("foo", null, KVMessage.StatusType.PUT_UPDATE),
                new DefaultKVMessage("foo", "error", KVMessage.StatusType.PUT_ERROR),
                new DefaultKVMessage("foo", null, KVMessage.StatusType.GET),
                new DefaultKVMessage("foo", "error", KVMessage.StatusType.GET_ERROR),
                new DefaultKVMessage("foo", "bar", KVMessage.StatusType.GET_SUCCESS),
                new DefaultKVMessage("foo", null, KVMessage.StatusType.DELETE),
                new DefaultKVMessage("foo", null, KVMessage.StatusType.DELETE_SUCCESS),
                new DefaultKVMessage("foo", "error", KVMessage.StatusType.DELETE_ERROR)
        };

        for (int i = 0; i < messages.length; i++) {
            KVMessage message = messages[i];

            byte[] encoded = Protocol.encode(message, i);

            CorrelatedMessage decoded = Protocol.decode(encoded);

            assertEquals(i, decoded.getCorrelationNumber());
            assertTrue(decoded.hasKVMessage());
            assertEquals(message.getStatus(), decoded.getKVMessage().getStatus());
            assertEquals(message.getKey(), decoded.getKVMessage().getKey());
            assertEquals(message.getValue(), decoded.getKVMessage().getValue());
        }
    }

    public void testEncodeDecodeAdminGenericResponse() throws ProtocolException {
        long correlationNumber = 1;

        // successful response

        GenericResponse successfulResponse = GenericResponse.success("yeah!");
        byte[] encoded = Protocol.encode(successfulResponse, correlationNumber);
        CorrelatedMessage decoded = Protocol.decode(encoded);

        assertTrue(decoded.hasAdminMessage());
        assertEquals(correlationNumber, decoded.getCorrelationNumber());
        GenericResponse decodedSuccessfulResponse = (GenericResponse) decoded.getAdminMessage();
        assertTrue(decodedSuccessfulResponse.isSuccess());
        assertEquals(successfulResponse.getMessage(), decodedSuccessfulResponse.getMessage());

        correlationNumber++;

        // unsuccessful response

        GenericResponse unsuccessfulResponse = GenericResponse.error("booh!");
        encoded = Protocol.encode(unsuccessfulResponse, correlationNumber);
        decoded = Protocol.decode(encoded);

        assertTrue(decoded.hasAdminMessage());
        assertEquals(correlationNumber, decoded.getCorrelationNumber());
        GenericResponse decodedUnsuccessfulResponse = (GenericResponse) decoded.getAdminMessage();
        assertFalse(decodedUnsuccessfulResponse.isSuccess());
        assertEquals(successfulResponse.getMessage(), decodedSuccessfulResponse.getMessage());
    }

    public void testEncodeDecodeAdminUpdateMetadataRequest() throws ProtocolException {
        long correlationNumber = 1;

        UpdateMetadataRequest request = new UpdateMetadataRequest();
        request.addNode(
                new InetSocketAddress("localhost", 42));
        request.addNode(
                new InetSocketAddress("www.google.de", 50000));
        request.addNode(
                new InetSocketAddress("127.0.0.1", 123));

        byte[] encoded = Protocol.encode(request, correlationNumber);
        CorrelatedMessage decoded = Protocol.decode(encoded);

        assertEquals(correlationNumber, decoded.getCorrelationNumber());
        assertTrue(decoded.hasAdminMessage());
        assertTrue(decoded.getAdminMessage() instanceof UpdateMetadataRequest);

        UpdateMetadataRequest decodedRequest = (UpdateMetadataRequest) decoded.getAdminMessage();

        Set<InetSocketAddress> expected = new HashSet<>(request.getNodes());
        Set<InetSocketAddress> actual = new HashSet<>(decodedRequest.getNodes());

        assertEquals(expected, actual);
    }

    public void testEncodeDecodeSimpleAdminMessages()
            throws ProtocolException, IllegalAccessException, InstantiationException {
        List<Class<? extends AdminMessage>> msgTypes = Arrays.asList(
                StopServerRequest.class,
                ShutDownServerRequest.class,
                EnableWriteLockRequest.class,
                DisableWriteLockRequest.class);

        long correlation = 0;
        for (Class<? extends AdminMessage> msgClass : msgTypes) {
            correlation++;

            AdminMessage msg = msgClass.newInstance();

            byte[] encoded = Protocol.encode(msg, correlation);

            CorrelatedMessage decoded = Protocol.decode(encoded);

            assertEquals(correlation, decoded.getCorrelationNumber());
            assertTrue(decoded.hasAdminMessage());
            assertEquals(msg.getClass(), decoded.getAdminMessage().getClass());
        }
    }

    public void testEncodeDecodeMoveDataRequest() throws ProtocolException {
        InetSocketAddress destination = new InetSocketAddress("somehost.de", 12345);
        Range range = new Range(0, 500);

        MoveDataRequest moveDataRequest = new MoveDataRequest(destination, range);

        long correlation = 0;
        byte[] encoded = Protocol.encode(moveDataRequest, correlation);

        CorrelatedMessage decoded = Protocol.decode(encoded);

        assertEquals(correlation, decoded.getCorrelationNumber());
        assertTrue(decoded.hasAdminMessage());
        assertEquals(MoveDataRequest.class, decoded.getAdminMessage().getClass());

        MoveDataRequest decodedRequest = (MoveDataRequest) decoded.getAdminMessage();

        assertEquals(destination, decodedRequest.getDestination());
        assertEquals(range, decodedRequest.getRange());
    }

    public void testEncodeDecodeExceptionMessage() throws ProtocolException {
        Exception cause = new RuntimeException("I fooed the bar.");
        ExceptionMessage exceptionMessage = new ExceptionMessage(cause);

        byte[] encoded = Protocol.encode(exceptionMessage, 42);

        CorrelatedMessage decoded = Protocol.decode(encoded);

        assertTrue(decoded.hasExceptionMessage());
        assertEquals(cause.getClass().getName(), decoded.getExceptionMessage().getExceptionClass());
        assertEquals(cause.getMessage(), decoded.getExceptionMessage().getMessage());
    }

    public void testEncodeDecodeMaintenanceStatusRequests() throws ProtocolException {
        long correlation = 1;

        // request
        GetMaintenanceStatusRequest request = new GetMaintenanceStatusRequest();
        byte[] encoded = Protocol.encode(request, correlation);
        CorrelatedMessage decoded = Protocol.decode(encoded);

        assertEquals(correlation, decoded.getCorrelationNumber());
        assertTrue(decoded.hasAdminMessage());
        assertEquals(GetMaintenanceStatusRequest.class, decoded.getAdminMessage().getClass());


        correlation++;

        // response
        MaintenanceStatusResponse response = new MaintenanceStatusResponse(true, "task", 42);
        encoded = Protocol.encode(response, correlation);
        decoded = Protocol.decode(encoded);

        assertEquals(correlation, decoded.getCorrelationNumber());
        assertTrue(decoded.hasAdminMessage());

        MaintenanceStatusResponse decodedResponse = (MaintenanceStatusResponse) decoded.getAdminMessage();
        assertEquals(response.isActive(), decodedResponse.isActive());
        assertEquals(response.getTask(), decodedResponse.getTask());
        assertEquals(response.getProgress(), decodedResponse.getProgress());
    }

    public void testEncodeDecodeGossipMessage() throws ProtocolException {
        long correlation = 1;

        Map<InetSocketAddress, ServerState> cluster = new HashMap<>();
        cluster.put(new InetSocketAddress("localhost", 50000),
                new ServerState(1, 1, ServerState.Status.OK, 1));
        cluster.put(new InetSocketAddress("localhost", 50001),
                new ServerState(2, 2, ServerState.Status.JOINING, 2));

        byte[] encoded = Protocol.encode(new ClusterDigest(cluster), correlation);
        CorrelatedMessage decoded = Protocol.decode(encoded);

        assertEquals(correlation, decoded.getCorrelationNumber());
        assertTrue(decoded.hasGossipMessage());
        assertTrue(decoded.getGossipMessage() instanceof ClusterDigest);
        ClusterDigest decodedDigest = (ClusterDigest) decoded.getGossipMessage();
        assertEquals(cluster, decodedDigest.getCluster());
    }

    public void testEncodeDecodeStreamMessages() throws ProtocolException {
        long correlation = 1;

        Map<InetSocketAddress, ServerState> cluster = new HashMap<>();
        cluster.put(new InetSocketAddress("localhost", 50000),
                new ServerState(1, 1, ServerState.Status.OK, 1));
        cluster.put(new InetSocketAddress("localhost", 50001),
                new ServerState(2, 2, ServerState.Status.OK, 2));
        ClusterDigest clusterDigest = new ClusterDigest(cluster);

        InetSocketAddress destination = new InetSocketAddress("localhost", 1234);
        Range targetRange = new Range(1, 42);
        InitiateStreamRequest initiateStreamRequest = new InitiateStreamRequest(destination, targetRange, clusterDigest, true);

        byte[] encoded = Protocol.encode(initiateStreamRequest, correlation);
        CorrelatedMessage decoded = Protocol.decode(encoded);
        assertEquals(correlation, decoded.getCorrelationNumber());
        assertTrue(decoded.hasAdminMessage());
        InitiateStreamRequest decodedInitiationRequest = (InitiateStreamRequest) decoded.getAdminMessage();
        assertEquals(destination, decodedInitiationRequest.getDestination());
        assertEquals(targetRange, decodedInitiationRequest.getKeyRange());
        assertEquals(clusterDigest, decodedInitiationRequest.getClusterDigest());
        assertTrue(decodedInitiationRequest.isMoveReplicationTarget());

        correlation++;

        // ok response
        InitiateStreamResponse okResp = new InitiateStreamResponse(true, "s1", 42, null);
        encoded = Protocol.encode(okResp, correlation);
        decoded = Protocol.decode(encoded);
        assertEquals(correlation, decoded.getCorrelationNumber());
        assertTrue(decoded.hasAdminMessage());
        InitiateStreamResponse decodedOkResponse = (InitiateStreamResponse) decoded.getAdminMessage();
        assertTrue(decodedOkResponse.isSuccess());
        assertEquals("s1", decodedOkResponse.getStreamId());
        assertEquals(42, decodedOkResponse.getNumberOfItems());
        assertNull(decodedOkResponse.getClusterDigest());

        correlation++;

        // error response
        InitiateStreamResponse errResp = new InitiateStreamResponse(false, "s2", 0, clusterDigest);
        encoded = Protocol.encode(errResp, correlation);
        decoded = Protocol.decode(encoded);
        assertEquals(correlation, decoded.getCorrelationNumber());
        assertTrue(decoded.hasAdminMessage());
        InitiateStreamResponse decodedErrResponse = (InitiateStreamResponse) decoded.getAdminMessage();
        assertFalse(decodedErrResponse.isSuccess());
        assertEquals("s2", decodedErrResponse.getStreamId());
        assertEquals(0, decodedErrResponse.getNumberOfItems());
        assertEquals(clusterDigest, decodedErrResponse.getClusterDigest());

        correlation++;

        // stream complete
        StreamCompleteMessage completeMessage = new StreamCompleteMessage("s1", targetRange);
        encoded = Protocol.encode(completeMessage, correlation);
        decoded = Protocol.decode(encoded);
        assertEquals(correlation, decoded.getCorrelationNumber());
        assertTrue(decoded.hasAdminMessage());
        StreamCompleteMessage decodedCompleteMsg = (StreamCompleteMessage) decoded.getAdminMessage();
        assertEquals("s1", decodedCompleteMsg.getStreamId());
        assertEquals(targetRange, decodedCompleteMsg.getRange());
    }

    public void testInitiateMRFlow() throws ProtocolException {
        long correlation = 1;

        InitiateMRRequest req1 = new InitiateMRRequest("mr1", "tgtNs", "foo()", new InetSocketAddress("host1", 123)
        );
        byte[] encoded = Protocol.encode(req1, correlation);
        CorrelatedMessage decoded = Protocol.decode(encoded);

        assertEquals(correlation, decoded.getCorrelationNumber());
        assertTrue(decoded.hasMRMessage());
        InitiateMRRequest decodedReq = (InitiateMRRequest) decoded.getMRMessage();
        assertEquals("mr1", decodedReq.getId());
        assertEquals("foo()", decodedReq.getScript());
        assertEquals("tgtNs", decodedReq.getTargetNamespace());

        correlation++;

        InitiateMRResponse resp = new InitiateMRResponse("mr1", null);
        encoded = Protocol.encode(resp, correlation);
        decoded = Protocol.decode(encoded);

        InitiateMRResponse decodedResp = (InitiateMRResponse) decoded.getMRMessage();
        assertEquals("mr1", decodedResp.getId());
        assertNull(decodedResp.getError());
    }

}