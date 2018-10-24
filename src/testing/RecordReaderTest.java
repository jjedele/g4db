package testing;

import common.utils.RecordReader;
import junit.framework.TestCase;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class RecordReaderTest extends TestCase {

    @Test
    public void testSplittingSmallData() throws IOException {
        byte[] data = "hello\nworld\nfoo\nbar\n".getBytes();
        RecordReader reader = new RecordReader(data, (byte) '\n');

        assertEquals("hello", new String(reader.read()));
        assertEquals("world", new String(reader.read()));
        assertEquals("foo", new String(reader.read()));
        assertEquals("bar", new String(reader.read()));
        assertNull(reader.read());
    }

    public void testSplittingBigData() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        for (int i = 0; i < 10000; i++) {
            bos.write("hello\n".getBytes(), 0, 6);
        }

        RecordReader reader = new RecordReader(bos.toByteArray(), (byte) '\n');
        for (int i = 0; i < 10000; i++) {
            assertEquals(
                    "Error in record " + i,
                    "hello",
                    new String(reader.read()));
        }

        assertNull(reader.read());
    }

}
