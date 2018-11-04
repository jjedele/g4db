package app_kvServer;

import app_kvServer.persistence.PersistenceException;
import app_kvServer.persistence.PersistenceService;
import common.Protocol;
import common.exceptions.ProtocolException;
import common.messages.DefaultKVMessage;
import common.messages.KVMessage;
import common.utils.RecordReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A ClientConnection represents an active session with a client application.
 */
public class ClientConnection implements Runnable {

    private static final Logger LOG = LogManager.getLogger(ClientConnection.class);

    private static final byte RECORD_SEPARATOR = 0x1e;

    private final AtomicBoolean running;
    private final PersistenceService persistenceService;
    private final SessionRegistry sessionRegistry;

    private final Socket socket;
    private InputStream inputStream;
    private OutputStream outputStream;

    /**
     * Default constructor.
     * @param clientSocket Socket for the client connection
     * @param persistenceService Instance of {@link PersistenceService} to use
     * @param sessionRegistry Instance of {@link SessionRegistry} to register with
     */
    public ClientConnection(Socket clientSocket,
                            PersistenceService persistenceService,
                            SessionRegistry sessionRegistry) {
        this.socket = clientSocket;
        this.running = new AtomicBoolean(false);
        this.persistenceService = persistenceService;
        this.sessionRegistry = sessionRegistry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        try {
            // TODO handle errors more gracefully
            this.running.set(true);
            sessionRegistry.registerSession(this);

            outputStream = socket.getOutputStream();
            inputStream = socket.getInputStream();
            RecordReader recordReader = new RecordReader(inputStream, RECORD_SEPARATOR);

            while (running.get()) {
                byte[] incoming;
                synchronized (recordReader) {
                    incoming = recordReader.read();
                }

                if (incoming == null) {
                    LOG.info("Terminating based on client's request.");
                    terminate();
                    break;
                }

                byte[] encodedResponse;
                try {
                    KVMessage msg = Protocol.decode(incoming);
                    KVMessage response = handleIncomingRequest(msg);
                    encodedResponse = Protocol.encode(response);
                } catch (ProtocolException e) {
                    LOG.error("Protocol exception.", e);
                    encodedResponse = Protocol.encode(e);
                }

                synchronized (outputStream) {
                    outputStream.write(encodedResponse);
                    outputStream.write(RECORD_SEPARATOR);
                    outputStream.flush();
                }
            }

            socket.close();
        } catch (IOException e) {
            LOG.error("Communication problem with client.", e);
        } finally {
            cleanConnectionShutdown();
            sessionRegistry.unregisterSession(this);
        }
    }

    /**
     * Terminate this session in a controlled manor.
     */
    public void terminate() {
        this.running.set(false);
    }

    private KVMessage handleIncomingRequest(KVMessage msg) throws ProtocolException {
        KVMessage reply;

        switch (msg.getStatus()) {
            case PUT:
                reply = handlePutRequest(msg);
                break;
            case GET:
                reply = handleGetRequest(msg);
                break;
            case DELETE:
                reply = handleDeleteRequest(msg);
                break;
            default:
                throw new ProtocolException("Unsupported request: " + msg.getStatus().name());
        }

        return reply;
    }

    private KVMessage handlePutRequest(KVMessage msg) {
        assert msg.getStatus() == KVMessage.StatusType.PUT;

        LOG.debug("Handling PUT request for key: {}", msg.getKey());

        KVMessage reply;

        try {
            boolean insert = persistenceService.put(
                    msg.getKey(),
                    msg.getValue());

            KVMessage.StatusType status =
                    insert ? KVMessage.StatusType.PUT_SUCCESS : KVMessage.StatusType.PUT_UPDATE;

            reply = new DefaultKVMessage(
                    msg.getKey(),
                    msg.getValue(),
                    status);
        } catch (PersistenceException e) {
            LOG.error("Error handling PUT request.", e);

            reply = new DefaultKVMessage(
                    msg.getKey(),
                    e.getMessage(),
                    KVMessage.StatusType.PUT_ERROR);
        }

        return reply;
    }

    private KVMessage handleGetRequest(KVMessage msg) {
        assert msg.getStatus() == KVMessage.StatusType.GET;

        LOG.debug("Handling GET request for key: {}", msg.getKey());

        KVMessage reply;

        try {
            String value = persistenceService.get(msg.getKey());

            reply = new DefaultKVMessage(
                    msg.getKey(),
                    value,
                    KVMessage.StatusType.GET_SUCCESS);
        } catch (PersistenceException e) {
            LOG.error("Error handling GET request.", e);

            reply = new DefaultKVMessage(
                    msg.getKey(),
                    e.getMessage(),
                    KVMessage.StatusType.GET_ERROR);
        }

        return reply;
    }

    private KVMessage handleDeleteRequest(KVMessage msg) {
        assert msg.getStatus() == KVMessage.StatusType.DELETE;

        LOG.debug("Handling DELETE request for key: {}", msg.getKey());

        KVMessage reply;

        try {
            persistenceService.delete(msg.getKey());

            reply = new DefaultKVMessage(
                    msg.getKey(),
                    msg.getValue(),
                    KVMessage.StatusType.DELETE_SUCCESS);
        } catch (PersistenceException e) {
            LOG.error("Error handling DELETE request.", e);

            reply = new DefaultKVMessage(
                    msg.getKey(),
                    e.getMessage(),
                    KVMessage.StatusType.DELETE_ERROR);
        }

        return reply;
    }

    private void cleanConnectionShutdown() {
        LOG.info("Closing connection.");
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException e) {
                LOG.error("Error closing connection.", e);
            }
        }
        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException e) {
                LOG.error("Error closing connection.", e);
            }
        }
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                LOG.error("Error closing connection.", e);
            }
        }
        running.set(false);
    }

}
