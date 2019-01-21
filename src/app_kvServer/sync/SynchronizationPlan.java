package app_kvServer.sync;

import common.hash.Range;
import common.utils.HostAndPort;

import java.util.*;

/**
 * Encapsulates a synchronization plan describing how to get missing data partitions
 * to a node.
 */
public class SynchronizationPlan {

    private final String reason;
    private final Map<Range, List<HostAndPort>> dataPartitions;

    /**
     * Constructor.
     * @param reason Description of the reason for the synchronization
     */
    public SynchronizationPlan(String reason) {
        this.reason = reason;
        this.dataPartitions = new HashMap<>();
    }

    /**
     * Add a range for synchronization.
     * @param range The range to synchronize to this node
     * @param sources A prioritized list of sources the range can be obtained from
     */
    public void add(Range range, HostAndPort... sources) {
        dataPartitions.put(range, Arrays.asList(sources));
    }

    /**
     * Return the partitions included in this synchronization plan.
     * @return
     */
    public Map<Range, List<HostAndPort>> getDataPartitions() {
        return Collections.unmodifiableMap(dataPartitions);
    }

    /**
     * Return the reason for this synchronization.
     * @return
     */
    public String getReason() {
        return reason;
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner("\n", "Synchronization Plan[\n", "\n]");
        dataPartitions.entrySet().forEach(e -> joiner.add(String.format("  %s\tfrom\t%s", e.getKey(), e.getValue())));
        return joiner.toString();
    }
}
