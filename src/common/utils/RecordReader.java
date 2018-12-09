package common.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Wraps a stream of binary data and extracts individual
 * records separated by a specified separator byte.
 */
public class RecordReader {

    public static final int BUFFER_SIZE = 8192;

    private final InputStream inputStream;
    private final byte separator;
    private final byte[] buffer;
    private final Queue<byte[]> outputBuffer;
    private ByteArrayOutputStream outputCollector;

    /**
     * Constructor.
     * @param inputStream InputStream to read from
     * @param separator The separator
     */
    public RecordReader(InputStream inputStream, byte separator) {
        this.inputStream = inputStream;
        this.separator = separator;
        this.buffer = new byte[BUFFER_SIZE];
        this.outputCollector = new ByteArrayOutputStream();
        this.outputBuffer = new LinkedList<>();
    }

    /**
     * Constructor.
     * @param data Data to parse
     * @param separator The separator
     */
    public RecordReader(byte[] data, byte separator) {
        this(new ByteArrayInputStream(data), separator);
    }

    /**
     * Read one record.
     * @return The record or null if there is none left in the provided data
     * @throws IOException If something goes wrong while reading
     */
    public byte[] read() throws IOException {
        if (!outputBuffer.isEmpty()) {
            // if we have records buffered from an earlier read
            // use those first
            return outputBuffer.remove();
        }

        int readCount;
        while ((readCount = inputStream.read(buffer)) >= 0) {
            // a read can possibly contain multiple records at once
            // we separate them and buffer them
            int recordStart = 0;
            for (int separatorPos : findSeparatorPositions(readCount)) {
                outputCollector.write(buffer, recordStart, separatorPos - recordStart);
                outputBuffer.add(outputCollector.toByteArray());
                outputCollector.close();
                outputCollector = new ByteArrayOutputStream();
                recordStart = separatorPos + 1;
            }

            // the current read might end with a started but incomplete record
            // if this is the case, buffer that data for the next read
            if (recordStart < readCount) {
                outputCollector.write(buffer, recordStart, readCount - recordStart);
            }

            // if the read contained a least one record we return it
            if (!outputBuffer.isEmpty()) {
                return outputBuffer.remove();
            }
        }

        // no more complete record in the data
        return null;
    }

    private List<Integer> findSeparatorPositions(int searchUntil) {
        List<Integer> positions = new ArrayList<>();
        for (int i = 0; i < searchUntil; i++) {
            if (buffer[i] == separator) {
                positions.add(i);
            }
        }
        return positions;
    }

}
