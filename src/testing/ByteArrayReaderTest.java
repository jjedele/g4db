package testing;

import common.utils.BinaryUtils;
import common.utils.ByteArrayReader;
import junit.framework.TestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ByteArrayReaderTest extends TestCase {

    public void testBasicReading() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        BinaryUtils.writeBool(bos, true);
        bos.write(Byte.MAX_VALUE);
        BinaryUtils.writeInt(bos, Integer.MAX_VALUE);
        BinaryUtils.writeLong(bos, Long.MAX_VALUE);
        BinaryUtils.writeString(bos, "foo");
        bos.write((byte) '\n');
        BinaryUtils.writeString(bos, "bar");

        ByteArrayReader reader = new ByteArrayReader(bos.toByteArray());

        assertTrue(reader.readBoolean());
        assertEquals(Byte.MAX_VALUE, reader.readByte());
        assertEquals(Integer.MAX_VALUE, reader.readInt());
        assertEquals(Long.MAX_VALUE, reader.readLong());
        assertEquals("foo", new String(reader.readUntil((byte) '\n')));
        assertEquals("bar", new String(reader.readRest()));
    }

}
