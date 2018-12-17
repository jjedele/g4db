package app_kvEcs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SimpleFailureDetector {

    private static final Logger LOG = LogManager.getLogger(SimpleFailureDetector.class);

    private final long threshold;
    private long lastTimestamp;
    private long lastTimestampChange;

    public SimpleFailureDetector(long threshold) {
        this.threshold = threshold;
    }

    public void heartbeat(long timestamp) {
        if (timestamp > lastTimestamp) {
            lastTimestamp = timestamp;
            lastTimestampChange = System.currentTimeMillis();
        }
    }

    public double getSuspicion() {
        long localTime = System.currentTimeMillis();
        long timeSinceLastChange = localTime - lastTimestampChange;
        return (double) timeSinceLastChange / threshold;
    }

}
