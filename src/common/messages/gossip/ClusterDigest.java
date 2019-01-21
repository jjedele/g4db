package common.messages.gossip;

import common.utils.HostAndPort;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Represents the accumulated state of the cluster.
 */
public class ClusterDigest implements GossipMessage {

    private final Map<HostAndPort, ServerState> cluster;

    /**
     * Constructor.
     *
     * @param cluster State of the cluster as collected by the Gossiper
     */
    public ClusterDigest(Map<HostAndPort, ServerState> cluster) {
        this.cluster = Collections.unmodifiableMap(new HashMap<>(cluster));
    }

    /**
     * Return the cluster state.
     *
     * @return A mapping of cluster nodes and their respective states
     */
    public Map<HostAndPort, ServerState> getCluster() {
        return cluster;
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner("\n", "[ClusterState:\n", "\n]");
        for (Map.Entry<HostAndPort, ServerState> entry : cluster.entrySet()) {
            joiner.add(String.format("%s (%s): Gen.: %d, Hbeat: %d, Sver: %d",
                    entry.getKey(), entry.getValue().getStatus(),
                    entry.getValue().getGeneration(), entry.getValue().getHeartBeat(),
                    entry.getValue().getStateVersion()));
        }
        return joiner.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ClusterDigest) {
            ClusterDigest cd = (ClusterDigest) obj;
            return cluster.equals(cd.cluster);
        } else {
            return false;
        }
    }

    /**
     * Return an empty cluster digest.
     * @return
     */
    public static ClusterDigest empty() {
        return new ClusterDigest(new HashMap<>());
    }

}
