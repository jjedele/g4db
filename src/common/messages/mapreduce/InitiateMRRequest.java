package common.messages.mapreduce;

import common.hash.Range;

import java.net.InetSocketAddress;

/**
 * Initiate a new map/reduce instance.
 */
public class InitiateMRRequest implements MRMessage {

    /** The type code for serialization. */
    public static final byte TYPE_CODE = 0x01;

    private final String id;
    private final Range sourceKeyRange;
    private final String sourceNamespace;
    private final String targetNamespace;
    private final String script;
    private final InetSocketAddress master;

    /**
     * Constructor.
     * @param id ID of the job.
     * @param sourceKeyRange Range of keys to be read.
     * @param sourceNamespace Namespace from which the source elements will be read.
     * @param targetNamespace Namespace to which the results will be written.
     * @param script The map/reduce script to execute.
     * @param master Address of the master. Might be null, then a new map/reduce instance is started.
     */
    public InitiateMRRequest(String id, Range sourceKeyRange, String sourceNamespace,
                             String targetNamespace, String script, InetSocketAddress master) {
        this.id = id;
        this.sourceKeyRange = sourceKeyRange;
        this.sourceNamespace = sourceNamespace;
        this.targetNamespace = targetNamespace;
        this.master = master;
        this.script = script;
    }

    /**
     * Return the ID of the job.
     * @return
     */
    public String getId() {
        return id;
    }

    /**
     * Return the script to execute.
     * @return
     */
    public String getScript() {
        return script;
    }

    /**
     * Return the source key range.
     * @return
     */
    public Range getSourceKeyRange() {
        return sourceKeyRange;
    }

    /**
     * Return the source namespace.
     * @return
     */
    public String getSourceNamespace() {
        return sourceNamespace;
    }

    /**
     * Return the namespace to which results will be written.
     * @return
     */
    public String getTargetNamespace() {
        return targetNamespace;
    }

    /**
     * Return the address of the map/reduce master.
     * @return
     */
    public InetSocketAddress getMaster() {
        return master;
    }

}
