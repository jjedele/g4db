package common.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Binary encodings for diverse Java primitives.
 */
public final class BinaryUtils {

    private BinaryUtils() {}

    /**
     * Encode a boolean to binary format.
     * @param b Boolean
     * @return Binary representation
     */
    public static byte encodeBool(boolean b) {
        return b ? (byte) 1 : (byte) 0;
    }

    /**
     * Encode a boolean and write it to the {@link OutputStream}.
     * @param os Output stream
     * @param b Boolean
     * @throws IOException
     */
    public static void writeBool(OutputStream os, boolean b) throws IOException {
        os.write(encodeBool(b));
    }

    /**
     * Encode an 32bit integer to binary format.
     * @param n Integer
     * @return Binary representation
     */
    public static byte[] encodeInt(int n) {
        return new byte[] {
                (byte) ((n >> 24) & 0xFF),
                (byte) ((n >> 16) & 0xFF),
                (byte) ((n >> 8) & 0xFF),
                (byte) (n & 0xFF)
        };
    }

    /**
     * Encode an integer and write it to the {@link OutputStream}.
     * @param os Output stream
     * @param n Integer
     * @throws IOException
     */
    public static void writeInt(OutputStream os, int n) throws IOException {
        byte[] data = encodeInt(n);
        os.write(data, 0, data.length);
    }

    /**
     * Encode an 64bit long integer to binary format.
     * @param n Long
     * @return Binary representation
     */
    public static byte[] encodeLong(long n) {
        return new byte[] {
                (byte) ((n >> 56) & 0xFF),
                (byte) ((n >> 48) & 0xFF),
                (byte) ((n >> 40) & 0xFF),
                (byte) ((n >> 32) & 0xFF),
                (byte) ((n >> 24) & 0xFF),
                (byte) ((n >> 16) & 0xFF),
                (byte) ((n >> 8) & 0xFF),
                (byte) (n & 0xFF)
        };
    }

    /**
     * Encode a long and write it to the {@link OutputStream}.
     * @param os Output stream
     * @param n Long
     * @throws IOException
     */
    public static void writeLong(OutputStream os, long n) throws IOException {
        byte[] data = encodeLong(n);
        os.write(data, 0, data.length);
    }

    /**
     * Encode an UTF8 string to binary format.
     * @param s String
     * @return Binary representation
     */
    public static byte[] encodeString(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Encode a string and write it to the {@link OutputStream}.
     * @param os Output stream
     * @param s String
     * @throws IOException
     */
    public static void writeString(OutputStream os, String s) throws IOException {
        byte[] data = encodeString(s);
        os.write(data, 0, data.length);
    }

}
