package common;

import common.messages.ExceptionMessage;
import common.messages.KVMessage;
import common.messages.admin.AdminMessage;
import common.messages.gossip.GossipMessage;
import common.messages.mapreduce.MRMessage;

/**
 * Holds the result of a protocol parsing operation.
 */
public class CorrelatedMessage {

    private final long correlationNumber;
    private final KVMessage kvMessage;
    private final AdminMessage adminMessage;
    private final ExceptionMessage exceptionMessage;
    private final GossipMessage gossipMessage;
    private final MRMessage mrMessage;

    /**
     * Constructor.
     * @param correlationNumber
     * @param kvMessage
     */
    public CorrelatedMessage(long correlationNumber, KVMessage kvMessage) {
        this.correlationNumber = correlationNumber;
        this.kvMessage = kvMessage;
        this.adminMessage = null;
        this.exceptionMessage = null;
        this.gossipMessage = null;
        this.mrMessage = null;
    }

    /**
     * Constructor.
     * @param correlationNumber
     * @param adminMessage
     */
    public CorrelatedMessage(long correlationNumber, AdminMessage adminMessage) {
        this.correlationNumber = correlationNumber;
        this.adminMessage = adminMessage;
        this.kvMessage = null;
        this.exceptionMessage = null;
        this.gossipMessage = null;
        this.mrMessage = null;
    }

    /**
     * Constructor.
     * @param correlationNumber
     * @param exceptionMessage
     */
    public CorrelatedMessage(long correlationNumber, ExceptionMessage exceptionMessage) {
        this.correlationNumber = correlationNumber;
        this.exceptionMessage = exceptionMessage;
        this.kvMessage = null;
        this.adminMessage = null;
        this.gossipMessage = null;
        this.mrMessage = null;
    }

    /**
     * Constructor.
     * @param correlationNumber
     * @param mrMessage
     */
    public CorrelatedMessage(long correlationNumber, MRMessage mrMessage) {
        this.correlationNumber = correlationNumber;
        this.exceptionMessage = null;
        this.kvMessage = null;
        this.adminMessage = null;
        this.gossipMessage = null;
        this.mrMessage = mrMessage;
    }

    /**
     * Constructor.
     * @param correlationNumber
     * @param gossipMessage
     */
    public CorrelatedMessage(long correlationNumber, GossipMessage gossipMessage) {
        this.correlationNumber = correlationNumber;
        this.gossipMessage = gossipMessage;
        this.kvMessage = null;
        this.adminMessage = null;
        this.exceptionMessage = null;
        this.mrMessage = null;
    }

    /**
     * Return if the parsing result is a KVMessage.
     * @return
     */
    public boolean hasKVMessage() {
        return kvMessage != null;
    }

    /**
     * Return if the parsing result is a AdminMessage.
     * @return
     */
    public boolean hasAdminMessage() {
        return adminMessage != null;
    }

    /**
     * Return if the parsing result is a ExceptionMessage.
     * @return
     */
    public boolean hasExceptionMessage() {
        return exceptionMessage != null;
    }

    /**
     * Return if the parsing result is a MRMessage.
     * @return
     */
    public boolean hasMRMessage() {
        return mrMessage != null;
    }

    /**
     * Return if the parsing result is a GossipMessage.
     * @return
     */
    public boolean hasGossipMessage() {
        return gossipMessage != null;
    }

    /**
     * Return the correlation number of this message.
     * @return
     */
    public long getCorrelationNumber() {
        return correlationNumber;
    }

    /**
     * Return the KVMessage associated with this ParsingResult.
     * @return
     */
    public KVMessage getKVMessage() {
        return kvMessage;
    }

    /**
     * Return the AdminMessage associated with this ParsingResult.
     * @return
     */
    public AdminMessage getAdminMessage() {
        return adminMessage;
    }

    /**
     * Return the ExceptionMessage associated with this ParsingResult.
     * @return
     */
    public ExceptionMessage getExceptionMessage() {
        return exceptionMessage;
    }

    /**
     * Return the GossipMessage associated with this ParsingResult.
     * @return
     */
    public GossipMessage getGossipMessage() {
        return gossipMessage;
    }

    /**
     * Return the MRMessage associated with this ParsingResult.
     * @return
     */
    public MRMessage getMRMessage() {
        return mrMessage;
    }

    @Override
    public String toString() {
        if (exceptionMessage != null) {
            return String.format("<%d:ExceptionMessage: %s>", correlationNumber, exceptionMessage.toString());
        } else if (kvMessage != null) {
            return String.format("<%d:KVMessage: %s>", correlationNumber, kvMessage.toString());
        } else if (adminMessage != null) {
            return String.format("<%d:AdminMessage: %s>", correlationNumber, adminMessage.toString());
        } else if (gossipMessage != null) {
            return String.format("<%d:GossipMessage: %s>", correlationNumber, adminMessage.toString());
        } else if (mrMessage != null) {
            return String.format("<%d:MRMessage: %s>", correlationNumber, adminMessage.toString());
        } else {
            return String.format("<%d:UNKNOWN>", correlationNumber);
        }
    }

}
