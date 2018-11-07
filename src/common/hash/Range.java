package common.hash;

/**
 * A numeric range between start and endpoint.
 */
public class Range {

    private final int start;
    private final int end;

    /**
     * Default constructor.
     * @param start Start of the range (exclusive)
     * @param end End of the range (inclusive)
     */
    public Range(int start, int end) {
        this.start = start;
        this.end = end;
    }

    /**
     * Get the start of the range.
     * @return Start (exclusive)
     */
    public int getStart() {
        return start;
    }

    /**
     * Get the end of the range.
     * @return End (inclusive)
     */
    public int getEnd() {
        return end;
    }

    /**
     * Check if a value in contained in this range.
     * @param val The value
     * @return true if value is contained
     */
    public boolean contains(int val) {
        return start < val && val <= end;
    }

}
