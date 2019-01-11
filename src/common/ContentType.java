package common;

/**
 *  ContentType defines the basic content of an encoded message.
 */
public final class ContentType {

    /**
     * A usual key,value message between client and server.
     */
    public static final byte KV_MESSAGE = 0x00;

    /**
     * A general server-side exception.
     */
    public static final byte EXCEPTION = 0x01;

    /**
     * Administrative message for server management.
     */
    public static final byte ADMIN = 0x02;

    /**
     * Server-server gossip communication.
     */
    public static final byte GOSSIP = 0x03;

    /**
     * Map/reduce related communication.
     */
    public static final byte MAP_REDUCE = 0x04;

}
