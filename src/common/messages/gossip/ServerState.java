package common.messages.gossip;

/**
 * Represents the status of one server instance.
 */
public class ServerState implements Comparable<ServerState> {

    public enum Status {
        STOPPED,
        OK,
        JOINING,
        DECOMMISSIONED
    }

    private final long generation;
    private final long heartBeat;
    private final Status status;
    private final long stateVersion;

    /**
     * Constructor.
     *
     * @param generation Server-local timestamp of when this server was started
     * @param heartBeat Server-local heartbeat
     * @param status Status of the server
     * @param stateVersion Version of the status
     */
    public ServerState(long generation, long heartBeat, Status status, long stateVersion) {
        this.generation = generation;
        this.heartBeat = heartBeat;
        this.status = status;
        this.stateVersion = stateVersion;
    }

    /**
     * Return the generation timestamp.
     *
     * @return Server-local timestamp
     */
    public long getGeneration() {
        return generation;
    }

    /**
     * Return the heartbeat timestamp.
     *
     * @return Server-local timestamp
     */
    public long getHeartBeat() {
        return heartBeat;
    }

    /**
     * Return the server status.
     *
     * @return Status
     */
    public Status getStatus() {
        return status;
    }

    /**
     * Return the version of the status update.
     *
     * @return Server-local timestamp
     */
    public long getStateVersion() {
        return stateVersion;
    }

    @Override
    public int compareTo(ServerState other) {
        if (this.generation != other.generation) {
            return Long.signum(this.generation - other.generation);
        } else if (this.heartBeat != other.heartBeat) {
            return Long.signum(this.heartBeat - other.heartBeat);
        } else {
            return Long.signum(this.stateVersion - other.stateVersion);
        }
    }

    @Override
    public int hashCode() {
        return (int) (generation + heartBeat + stateVersion + status.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ServerState) {
            ServerState other = (ServerState) obj;
            return (this.generation == other.generation)
                    && (this.heartBeat == other.heartBeat)
                    && (this.status == other.status)
                    && (this.stateVersion == other.stateVersion);
        } else {
            return false;
        }
    }
}
