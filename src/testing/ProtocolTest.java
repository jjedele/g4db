package testing;

import common.Protocol;
import common.exceptions.ProtocolException;
import common.messages.DefaultKVMessage;
import common.messages.KVMessage;
import junit.framework.TestCase;

public class ProtocolTest extends TestCase {

    //PUT
    public void testEncodeDecodePut() throws ProtocolException {
        KVMessage msg = new DefaultKVMessage("foo", "bar", KVMessage.StatusType.PUT);

        byte[] payload = Protocol.encode(msg);

        KVMessage msg2 = Protocol.decode(payload);

        assertEquals(msg.getKey(), msg2.getKey());
        assertEquals(msg.getValue(), msg2.getValue());
        assertEquals(msg.getStatus(), msg2.getStatus());
    }

    // PUT_SUCCESS
    public void testEncodeDecodePutSuccess() throws ProtocolException {
        KVMessage msg = new DefaultKVMessage("foo", "bar", KVMessage.StatusType.PUT_SUCCESS);

        byte[] payload = Protocol.encode(msg);

        KVMessage msg2 = Protocol.decode(payload);

        assertEquals(msg.getKey(), msg2.getKey());
        assertEquals(msg.getValue(), msg2.getValue());
        assertEquals(msg.getStatus(), msg2.getStatus());
    }

    // PUT_ERROR
    public void testEncodeDecodePutError() throws ProtocolException {
        KVMessage msg = new DefaultKVMessage("foo", "bar", KVMessage.StatusType.PUT_ERROR);

        byte[] payload = Protocol.encode(msg);

        KVMessage msg2 = Protocol.decode(payload);

        assertEquals(msg.getKey(), msg2.getKey());
        assertEquals(msg.getValue(), msg2.getValue());
        assertEquals(msg.getStatus(), msg2.getStatus());
    }

    // PUT_UPDATE
    public void testEncodeDecodePutUpdate() throws ProtocolException {
        KVMessage msg = new DefaultKVMessage("foo", "bar", KVMessage.StatusType.PUT_UPDATE);

        byte[] payload = Protocol.encode(msg);

        KVMessage msg2 = Protocol.decode(payload);

        assertEquals(msg.getKey(), msg2.getKey());
        assertEquals(msg.getValue(), msg2.getValue());
        assertEquals(msg.getStatus(), msg2.getStatus());
    }

    //GET
    public void testEncodeDecodeGet() throws ProtocolException {
        KVMessage msg = new DefaultKVMessage("foo", null, KVMessage.StatusType.GET);

        byte[] payload = Protocol.encode(msg);

        KVMessage msg2 = Protocol.decode(payload);

        assertEquals(msg.getKey(), msg2.getKey());
        assertEquals(msg.getValue(), msg2.getValue());
        assertEquals(msg.getStatus(), msg2.getStatus());
    }

    //GET_ERROR
    public void testEncodeDecodeGetError() throws ProtocolException {
        KVMessage msg = new DefaultKVMessage("foo", "bar", KVMessage.StatusType.GET_ERROR);

        byte[] payload = Protocol.encode(msg);

        KVMessage msg2 = Protocol.decode(payload);

        assertEquals(msg.getKey(), msg2.getKey());
        assertEquals(msg.getValue(), msg2.getValue());
        assertEquals(msg.getStatus(), msg2.getStatus());
    }

    //GET_SUCCESS
    public void testEncodeDecodeGetSuccess() throws ProtocolException {
        KVMessage msg = new DefaultKVMessage("foo", "bar", KVMessage.StatusType.GET_SUCCESS);

        byte[] payload = Protocol.encode(msg);

        KVMessage msg2 = Protocol.decode(payload);

        assertEquals(msg.getKey(), msg2.getKey());
        assertEquals(msg.getValue(), msg2.getValue());
        assertEquals(msg.getStatus(), msg2.getStatus());
    }

    // DELETE
    public void testEncodeDecodeDelete() throws ProtocolException {
        KVMessage msg = new DefaultKVMessage("foo", null, KVMessage.StatusType.DELETE);

        byte[] payload = Protocol.encode(msg);

        KVMessage msg2 = Protocol.decode(payload);

        assertEquals(msg.getKey(), msg2.getKey());
        assertEquals(msg.getValue(), msg2.getValue());
        assertEquals(msg.getStatus(), msg2.getStatus());
    }

    // DELETE_SUCCESS
    public void testEncodeDecodeDeleteSuccess() throws ProtocolException {
        KVMessage msg = new DefaultKVMessage("foo", "bar", KVMessage.StatusType.DELETE_SUCCESS);

        byte[] payload = Protocol.encode(msg);

        KVMessage msg2 = Protocol.decode(payload);

        assertEquals(msg.getKey(), msg2.getKey());
        assertEquals(msg.getValue(), msg2.getValue());
        assertEquals(msg.getStatus(), msg2.getStatus());
    }

    // DELETE_ERROR
    public void testEncodeDecodeDeleteError() throws ProtocolException {
        KVMessage msg = new DefaultKVMessage("foo", "bar", KVMessage.StatusType.DELETE_ERROR);

        byte[] payload = Protocol.encode(msg);

        KVMessage msg2 = Protocol.decode(payload);

        assertEquals(msg.getKey(), msg2.getKey());
        assertEquals(msg.getValue(), msg2.getValue());
        assertEquals(msg.getStatus(), msg2.getStatus());
    }

}