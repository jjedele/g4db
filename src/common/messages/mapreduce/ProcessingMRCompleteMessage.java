package common.messages.mapreduce;

import common.hash.Range;

import java.util.Map;

/**
 * Message that is sent to the map/reduce master after the worker finished
 * processing the data it is responsible for.
 */
public class ProcessingMRCompleteMessage implements MRMessage {

    /** The type code for serialization. */
    public static final byte TYPE_CODE = 0x03;

    private final String id;
    private final Range range;
    // FIXME putting all results in one msg is not scalable, but regarding time it will have to suffice
    // FIXME a better options would be implementing cursors
    private final Map<String, String> results;

    /**
     * Constructor.
     *
     * @param id ID of the map/reduce job.
     * @param range Key-range that has been processed.
     * @param results Results of the processing.
     */
    public ProcessingMRCompleteMessage(String id, Range range, Map<String, String> results) {
        this.id = id;
        this.range = range;
        this.results = results;
    }

    /**
     * Return the ID of the job.
     *
     * @return job ID
     */
    public String getId() {
        return id;
    }

    /**
     * Return the key-range that has been processed.
     *
     * @return Key range.
     */
    public Range getRange() {
        return range;
    }

    /**
     * Return the processing results.
     *
     * @return Processing results by key.
     */
    public Map<String, String> getResults() {
        return results;
    }

}
