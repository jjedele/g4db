package testing;

import common.Protocol;
import common.exceptions.ProtocolException;
import common.messages.DefaultKVMessage;
import common.messages.KVMessage;
import junit.framework.TestCase;

public class ProtocolTest extends TestCase {

    public void testEncodeDecodePut() throws ProtocolException {
        KVMessage msg = new DefaultKVMessage("foo", "bar", KVMessage.StatusType.PUT);

        byte[] payload = Protocol.encode(msg);

        KVMessage msg2 = Protocol.decode(payload);

        assertEquals(msg.getKey(), msg2.getKey());
        assertEquals(msg.getValue(), msg2.getValue());
        assertEquals(msg.getStatus(), msg2.getStatus());
   }

}
