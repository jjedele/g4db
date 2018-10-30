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

    //GET
    public void testEncodeDecodeGet() throws ProtocolException {
        KVMessage msg = new DefaultKVMessage("foo", null, KVMessage.StatusType.GET);

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

        assertEquals(msg.putSuccessKey(), msg2.putSuccessKey());
        assertEquals(msg.putSuccessValue(), msg2.putSuccessValue());
        assertEquals(msg.putSuccessStatus(), msg2.putSuccessStatus());
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

    // DELETE_SUCCESS
    public void testEncodeDecodeDeleteSuccess() throws ProtocolException {
        KVMessage msg = new DefaultKVMessage("foo", "bar", KVMessage.StatusType.DELETE_SUCCESS);

        byte[] payload = Protocol.encode(msg);

        KVMessage msg2 = Protocol.decode(payload);

        assertEquals(msg.DeleteSuccessKey(), msg2.DeleteSuccessKey());
        assertEquals(msg.DeleteSuccessValue(), msg2.DeleteSuccessValue());
        assertEquals(msg.DeleteSuccessStatus(), msg2.DeleteSuccessStatus());

    }

}