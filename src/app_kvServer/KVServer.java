package app_kvServer;


import app_kvServer.persistence.DummyPersistenceService;
import app_kvServer.persistence.PersistenceService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

public class KVServer implements Runnable, SessionRegistry {

    private static final Logger LOG = LogManager.getLogger(KVServer.class);

    private final int port;
    private final int cacheSize;
    private final CacheReplacementStrategy cacheStrategy;
    private final Set<ClientConnection> activeSessions;

    /**
     * Entry point for the server.
     * @param args Configuration for the server. In order:
     *             port - defaults to 12345
     *             cache size - defaults to 10000
     *             cache strategy - can be one of FIFO, LRU, LFU
     */
    public static void main(String[] args) {
        int port = 50000;
        int cacheSize = 10000;
        CacheReplacementStrategy strategy = CacheReplacementStrategy.FIFO;

        if (args.length >= 1) {
            try {
                port = Integer.parseUnsignedInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("First argument (port) must be a positive integer.");
                System.exit(1);
            }
        }
        if (args.length >= 2) {
            try {
                cacheSize = Integer.parseUnsignedInt(args[1]);
            } catch (NumberFormatException e) {
                System.err.println("Second argument (cache size) must be a positive integer.");
                System.exit(1);
            }
        }
        if (args.length >= 3) {
            try {
                strategy = CacheReplacementStrategy.valueOf(args[2]);
            } catch (IllegalArgumentException e) {
                System.err.println("Third argument (cache strategy) must be FIFO, LRU or LFU.");
                System.exit(1);
            }
        }

        KVServer server = new KVServer(port, cacheSize, strategy);
        new Thread(server).run();
    }

    /**
     * Start KV Server at given port
     *
     * @param port      given port for persistence server to operate
     * @param cacheSize specifies how many key-value pairs the server is allowed
     *                  to keep in-memory
     * @param cacheStrategy  specifies the cache replacement strategy in case the cache
     *                  is full and there is a GET- or PUT-request on a key that is
     *                  currently not contained in the cache.
     */
    public KVServer(int port, int cacheSize, CacheReplacementStrategy cacheStrategy) {
        this.port = port;
        this.cacheSize = cacheSize;
        this.cacheStrategy = cacheStrategy;
        this.activeSessions = new HashSet<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        // TODO handle errors more gracefully
        try {
            PersistenceService persistenceService = new DummyPersistenceService(cacheSize, cacheStrategy);

            ServerSocket serverSocket = new ServerSocket(port);
            LOG.info("Server listening on port {}.", port);

            // accept client connections
            while (true) {
                Socket clientSocket = serverSocket.accept();

                ClientConnection clientConnection = new ClientConnection(
                        clientSocket,
                        persistenceService,
                        this);

                new Thread(clientConnection).start();
            }
        } catch (IOException e) {
            LOG.error("Error while accepting connections.", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerSession(ClientConnection session) {
        activeSessions.add(session);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregisterSession(ClientConnection session) {
        activeSessions.remove(session);
    }

}
