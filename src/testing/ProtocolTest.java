package testing;

import common.DecodingResult;
import common.Protocol;
import common.exceptions.ProtocolException;
import common.hash.NodeEntry;
import common.hash.Range;
import common.messages.DefaultKVMessage;
import common.messages.ExceptionMessage;
import common.messages.KVMessage;
import common.messages.admin.GenericResponse;
import common.messages.admin.UpdateMetadataRequest;
import junit.framework.TestCase;

import java.net.InetSocketAddress;
import java.util.HashSet;
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

            DecodingResult decoded = Protocol.decode(encoded);

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
        DecodingResult decoded = Protocol.decode(encoded);

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
        request.addNode(new NodeEntry(
                "anton",
                new InetSocketAddress("localhost", 42),
                new Range(0, 4)));
        request.addNode(new NodeEntry(
                "berta",
                new InetSocketAddress("www.google.de", 50000),
                new Range(5, 7)));
        request.addNode(new NodeEntry(
                "sigmund",
                new InetSocketAddress("127.0.0.1", 123),
                new Range(400, 500)));

        byte[] encoded = Protocol.encode(request, correlationNumber);
        DecodingResult decoded = Protocol.decode(encoded);

        assertEquals(correlationNumber, decoded.getCorrelationNumber());
        assertTrue(decoded.hasAdminMessage());
        assertTrue(decoded.getAdminMessage() instanceof UpdateMetadataRequest);

        UpdateMetadataRequest decodedRequest = (UpdateMetadataRequest) decoded.getAdminMessage();

        Set<NodeEntry> expected = new HashSet<>(request.getNodes());
        Set<NodeEntry> actual = new HashSet<>(decodedRequest.getNodes());

        assertEquals(expected, actual);
    }

    public void testEncodeDecodeExceptionMessage() throws ProtocolException {
        Exception cause = new RuntimeException("I fooed the bar.");
        ExceptionMessage exceptionMessage = new ExceptionMessage(cause);

        byte[] encoded = Protocol.encode(exceptionMessage, 42);

        DecodingResult decoded = Protocol.decode(encoded);

        assertTrue(decoded.hasExceptionMessage());
        assertEquals(cause.getClass().getName(), decoded.getExceptionMessage().getExceptionClass());
        assertEquals(cause.getMessage(), decoded.getExceptionMessage().getMessage());
    }

}