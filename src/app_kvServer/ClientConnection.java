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

    private final Socket clientSocket;
    private final AtomicBoolean terminated;
    private final PersistenceService persistenceService;
    private final SessionRegistry sessionRegistry;

    /**
     * Default constructor.
     * @param clientSocket Socket for the client connection
     * @param persistenceService Instance of {@link PersistenceService} to use
     * @param sessionRegistry Instance of {@link SessionRegistry} to register with
     */
    public ClientConnection(Socket clientSocket,
                            PersistenceService persistenceService,
                            SessionRegistry sessionRegistry) {
        this.clientSocket = clientSocket;
        this.terminated = new AtomicBoolean(false);
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
            sessionRegistry.registerSession(this);

            OutputStream outputStream = clientSocket.getOutputStream();
            InputStream inputStream = clientSocket.getInputStream();
            RecordReader recordReader = new RecordReader(inputStream, (byte) '\n');

            while (!terminated.get()) {
                byte[] payload = recordReader.read();

                if (payload[0] == (byte) 'Q') {
                    LOG.info("Terminating based on client's request.");
                    terminate();
                    break;
                }

                byte[] response;
                try {
                    response = handleMessage(payload);
                } catch (PersistenceException | ProtocolException e) {
                    response = ("ERROR: " + e.getMessage() + "\n").getBytes();
                }

                outputStream.write(response);
                outputStream.flush();
            }

            clientSocket.close();
        } catch (IOException e) {
           LOG.error("Communication problem with client.", e);
        } finally {
            sessionRegistry.unregisterSession(this);
        }
    }

    /**
     * Terminate this session in a controlled manor.
     */
    public void terminate() {
        this.terminated.set(true);
    }

    private byte[] handleMessage(byte[] payload) throws ProtocolException, PersistenceException, IOException {
        // TODO: this must be extended as soon we have implemented the full wire protocol
        KVMessage message = Protocol.decode(payload);

        if (message.getStatus() == KVMessage.StatusType.PUT) {
            persistenceService.persist(message.getKey(), message.getValue());
            KVMessage response = new DefaultKVMessage(
                    message.getKey(),
                    message.getValue(),
                    KVMessage.StatusType.PUT_SUCCESS);
            return Protocol.encode(response);
        } else if (message.getStatus() == KVMessage.StatusType.GET) {
            String value = persistenceService.get(message.getKey());
            KVMessage response = new DefaultKVMessage(
                    message.getKey(),
                    value,
                    KVMessage.StatusType.GET_SUCCESS);
            return Protocol.encode(response);
        } else {
            throw new ProtocolException("Unsupported message status: " + message.getStatus());
        }
    }

}
