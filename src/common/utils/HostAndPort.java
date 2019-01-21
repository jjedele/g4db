package common.utils;

import org.apache.logging.log4j.message.StringFormattedMessage;

/**
 * Replacement for {@link java.net.InetSocketAddress}.
 *
 * This version does not attempt any resolving operations.
 */
public class HostAndPort {

    private final String host;
    private final int port;

    /**
     * Constructor.
     *
     * @param host Host address.
     * @param port Port.
     */
    public HostAndPort(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Return the host.
     *
     * @return
     */
    public String getHost() {
        return host;
    }

    /**
     * Return the port.
     * @return
     */
    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return String.format("%s:%d", host, port);
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof HostAndPort) {
            HostAndPort other = (HostAndPort) obj;
            return this.host.equals(other.host) && this.port == other.port;
        } else {
            return false;
        }
    }
    
}
