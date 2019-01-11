package common.messages.mapreduce;

import java.net.InetSocketAddress;

/**
 * Initiate a new map/reduce instance.
 */
public class InitiateMRRequest implements MRMessage {

    /** The type code for serialization. */
    public static final byte TYPE_CODE = 0x01;

    private final String id;
    private final String targetNamespace;
    private final String script;
    private final InetSocketAddress master;

    /**
     * Constructor.
     * @param id ID of the job.
     * @param targetNamespace Namespace to which the results will be written.
     * @param script The map/reduce script to execute.
     * @param master Address of the master. Might be null, then a new map/reduce instance is started.
     */
    public InitiateMRRequest(String id, String targetNamespace, String script, InetSocketAddress master) {
        this.id = id;
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
