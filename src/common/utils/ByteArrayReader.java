package common.utils;

/**
 * Controlled access to parts of byte arrays.
 */
public class ByteArrayReader {

    private final byte[] data;
    private int pos;

    /**
     * Default constructor.
     * @param data Array to operate upon
     */
    public ByteArrayReader(byte[] data) {
        this.data = data;
        this.pos = 0;
    }

    /**
     * Read a single byte.
     * @return The byte
     */
    public byte readByte() {
        ensureEnoughData(1);
        return data[pos++];
    }

    /**
     * Read a boolean.
     * @return The boolean
     */
    public boolean readBoolean() {
        return readByte() != (byte) 0;
    }

    /**
     * Read a 32bit integer.
     * @return The integer
     */
    public int readInt() {
        ensureEnoughData(4);
        return (data[pos++] & 0xFF) << 24 |
                (data[pos++] & 0xFF) << 16 |
                (data[pos++] & 0xFF) << 8 |
                (data[pos++] & 0xFF);
    }

    /**
     * Read a 64bit long integer.
     * @return The long
     */
    public long readLong() {
        ensureEnoughData(8);
        return ((long) data[pos++] & 0xFF) << 56 |
                ((long) data[pos++] & 0xFF) << 48 |
                ((long) data[pos++] & 0xFF) << 40 |
                ((long) data[pos++] & 0xFF) << 32 |
                ((long) data[pos++] & 0xFF) << 24 |
                ((long) data[pos++] & 0xFF) << 16 |
                ((long) data[pos++] & 0xFF) << 8 |
                ((long) data[pos++] & 0xFF);
    }

    /**
     * Read data until given separator byte is encountered.
     *
     * The position will be set behind the separator byte.
     *
     * @param sep Byte that separates the next record.
     * @return The bytes up to but not including the separator byte
     */
    public byte[] readUntil(byte sep) {
        int sepPos = pos;
        while (sepPos < data.length && data[sepPos] != sep) {
            sepPos++;
        }
        if (sepPos >= data.length) {
            throw new RuntimeException("Separator not found.");
        }

        byte[] part = new byte[sepPos - pos];
        System.arraycopy(data, pos, part, 0, part.length);

        pos = sepPos + 1;

        return part;
    }

    /**
     * Read the bytes from the current position to the end.
     * @return Array of bytes
     */
    public byte[] readRest() {
        byte[] rest = new byte[data.length - pos];
        System.arraycopy(data, pos, rest, 0, rest.length);
        pos = data.length;
        return rest;
    }

    /**
     * Return if this reader has data left.
     * @return True if data left
     */
    public boolean hasRemainingData() {
        return pos < data.length;
    }

    private void ensureEnoughData(int n) {
        if (pos + n > data.length) {
            throw new RuntimeException("Not enough data left.");
        }
    }

}
