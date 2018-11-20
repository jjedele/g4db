package testing;

import common.CorrelatedMessage;
import common.Protocol;
import common.exceptions.ProtocolException;
import common.hash.NodeEntry;
import common.hash.Range;
import common.messages.DefaultKVMessage;
import common.messages.ExceptionMessage;
import common.messages.KVMessage;
import common.messages.admin.*;
import junit.framework.TestCase;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
                StartServerRequest.class,
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

}