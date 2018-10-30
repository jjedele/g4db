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

        assertEquals(msg.putKey(), msg2.putValue());
        assertEquals(msg.putValue(), msg2.putValue());
        assertEquals(msg.putStatus(), msg2.putStatus());
    }

    // PUT_SUCCESS
    public void testEncodeDecodePutSuccess() throws ProtocolException {
        KVMessage msg = new DefaultKVMessage("foo", "bar", KVMessage.StatusType.PUT_SUCCESS);

        byte[] payload = Protocol.encode(msg);

        KVMessage msg2 = Protocol.decode(payload);

        assertEquals(msg.putSuccessKey(), msg2.putSuccessKey());
        assertEquals(msg.putSuccessValue(), msg2.putSuccessValue());
        assertEquals(msg.putSuccessStatus(), msg2.putSuccessStatus());
    }

    // PUT_ERROR
    public void testEncodeDecodePutError() throws ProtocolException {
        KVMessage msg = new DefaultKVMessage("foo", "bar", KVMessage.StatusType.PUT_ERROR);

        byte[] payload = Protocol.encode(msg);

        KVMessage msg2 = Protocol.decode(payload);

        assertEquals(msg.putErrorKey(), msg2.putErrorKey());
        assertEquals(msg.putErrorValue(), msg2.putErrorValue());
        assertEquals(msg.putErrorStatus(), msg2.putErrorStatus());
    }

    // PUT_UPDATE
    public void testEncodeDecodePutUpdate() throws ProtocolException {
        KVMessage msg = new DefaultKVMessage("foo", "bar", KVMessage.StatusType.PUT_UPDATE);

        byte[] payload = Protocol.encode(msg);

        KVMessage msg2 = Protocol.decode(payload);

        assertEquals(msg.putUpdateKey(), msg2.putUpdateKey());
        assertEquals(msg.putUpdateValue(), msg2.putUpdateValue());
        assertEquals(msg.putUpdateStatus(), msg2.putUpdateStatus());
    }

    //GET
    public void testEncodeDecodeGet() throws ProtocolException {
        KVMessage msg = new DefaultKVMessage("foo", "bar", KVMessage.StatusType.GET);

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

        assertEquals(msg.getErrorKey(), msg2.getErrorKey());
        assertEquals(msg.getErrorValue(), msg2.getErrorValue());
        assertEquals(msg.getErrorStatus(), msg2.getErrorStatus());
    }

    //GET_SUCCESS
    public void testEncodeDecodeGetSuccess() throws ProtocolException {
        KVMessage msg = new DefaultKVMessage("foo", "bar", KVMessage.StatusType.GET_SUCCESS);

        byte[] payload = Protocol.encode(msg);

        KVMessage msg2 = Protocol.decode(payload);

        assertEquals(msg.getSuccessKey(), msg2.getSuccessKey());
        assertEquals(msg.getSuccessValue(), msg2.getSuccessValue());
        assertEquals(msg.getSuccessStatus(), msg2.getSuccessStatus());
    }

    // DELETE
    public void testEncodeDecodeDelete() throws ProtocolException {
        KVMessage msg = new DefaultKVMessage("foo", "bar", KVMessage.StatusType.DELETE);

        byte[] payload = Protocol.encode(msg);

        KVMessage msg2 = Protocol.decode(payload);

        assertEquals(msg.deleteKey(), msg2.deleteKey());
        assertEquals(msg.deleteValue(), msg2.deleteValue());
        assertEquals(msg.deleteStatus(), msg2.deleteStatus());
    }

    // DELETE_SUCCESS
    public void testEncodeDecodeDeleteSuccess() throws ProtocolException {
        KVMessage msg = new DefaultKVMessage("foo", "bar", KVMessage.StatusType.DELETE_SUCCESS);

        byte[] payload = Protocol.encode(msg);

        KVMessage msg2 = Protocol.decode(payload);

        assertEquals(msg.deleteSuccessKey(), msg2.deleteSuccessKey());
        assertEquals(msg.deleteSuccessValue(), msg2.deleteSuccessValue());
        assertEquals(msg.deleteSuccessStatus(), msg2.deleteSuccessStatus());
    }

    // DELETE_ERROR
    public void testEncodeDecodeDeleteError() throws ProtocolException {
        KVMessage msg = new DefaultKVMessage("foo", "bar", KVMessage.StatusType.DELETE_ERROR);

        byte[] payload = Protocol.encode(msg);

        KVMessage msg2 = Protocol.decode(payload);

        assertEquals(msg.deleteErrorKey(), msg2.deleteErrorKey());
        assertEquals(msg.deleteErrorValue(), msg2.deleteErrorValue());
        assertEquals(msg.deleteErrorStatus(), msg2.deleteErrorStatus());
    }

}