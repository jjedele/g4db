package app_kvServer;

import app_kvServer.admin.AdminTasks;
import app_kvServer.admin.MoveDataTask;
import app_kvServer.persistence.PersistenceException;
import app_kvServer.persistence.PersistenceService;
import common.CorrelatedMessage;
import common.Protocol;
import common.exceptions.ProtocolException;
import common.hash.NodeEntry;
import common.hash.Range;
import common.messages.DefaultKVMessage;
import common.messages.ExceptionMessage;
import common.messages.KVMessage;
import common.messages.Message;
import common.messages.admin.*;
import common.utils.RecordReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A ClientConnection represents an active session with a client application.
 */
public class ClientConnection implements Runnable {

    private static final Logger LOG = LogManager.getLogger(ClientConnection.class);

    private static final byte RECORD_SEPARATOR = 0x1e;

    private final AtomicBoolean running;
    private final PersistenceService persistenceService;
    private final SessionRegistry sessionRegistry;
    private final ServerState serverState;

    private final Socket socket;
    private InputStream inputStream;
    private OutputStream outputStream;

    /**
     * Default constructor.
     * @param clientSocket Socket for the client connection
     * @param persistenceService Instance of {@link PersistenceService} to use
     * @param sessionRegistry Instance of {@link SessionRegistry} to register with
     * @param serverState Global server state
     */
    public ClientConnection(Socket clientSocket,
                            PersistenceService persistenceService,
                            SessionRegistry sessionRegistry,
                            ServerState serverState) {
        this.socket = clientSocket;
        this.running = new AtomicBoolean(false);
        this.persistenceService = persistenceService;
        this.sessionRegistry = sessionRegistry;
        this.serverState = serverState;
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

            ThreadContext.put("client", socket.getRemoteSocketAddress().toString());
            LOG.info("Established connection with client.");

            while (running.get()) {
                byte[] incoming = recordReader.read();

                if (incoming == null) {
                    LOG.info("Terminating based on client's request.");
                    terminate();
                    break;
                } else if (incoming.length == 0) {
                    // client keep alive, ignore
                    continue;
                }

                Message response;
                long correlationNumber = 0;
                try {
                    CorrelatedMessage request = Protocol.decode(incoming);
                    correlationNumber = request.getCorrelationNumber();
                    response = handleIncomingMessage(request);
                } catch (ProtocolException e) {
                    LOG.error("Protocol exception.", e);
                    response = new ExceptionMessage(e);
                }

                byte[] outgoing = Protocol.encode(response, correlationNumber);
                outputStream.write(outgoing);
                outputStream.write(RECORD_SEPARATOR);
                outputStream.flush();

                ThreadContext.remove("correlation");
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

    private Message handleIncomingMessage(CorrelatedMessage request) throws ProtocolException {
        ThreadContext.put("correlation", Long.toUnsignedString(request.getCorrelationNumber()));

        if (request.hasKVMessage()) {
            return handleIncomingKVRequest(request.getKVMessage());
        } else if (request.hasAdminMessage()) {
            return handleAdminMessage(request.getAdminMessage());
        } else {
            throw new ProtocolException("Unsupported request: " + request);
        }
    }

    private KVMessage handleIncomingKVRequest(KVMessage msg) throws ProtocolException {
        // ensure the server is currently not stopped and return appropriate message otherwise
        if (serverState.isStopped()) {
            return new DefaultKVMessage(msg.getKey(), null, KVMessage.StatusType.SERVER_STOPPED);
        }

        // ensure we're responsible
        InetSocketAddress responsibleNode = serverState.getClusterNodes().getResponsibleNode(msg.getKey());
        if (!serverState.getMyself().equals(responsibleNode)) {
            LOG.info("Sending client {} NOT_RESPONSIBLE message with updated nodes.");
            List<NodeEntry> nodes = serverState.getClusterNodes().getNodes().stream()
                    .map(address -> new NodeEntry("somenode", address, new Range(0, 1)))
                    .collect(Collectors.toList());
            return new DefaultKVMessage(
                    msg.getKey(),
                    NodeEntry.multipleToSerializableString(nodes),
                    KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
        }

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

        // ensure the write lock of the server is currently not enabled
        if (serverState.isWriteLockActive()) {
            return new DefaultKVMessage(msg.getKey(), null, KVMessage.StatusType.SERVER_WRITE_LOCK);
        }

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

        // ensure the write lock of the server is currently not enabled
        if (serverState.isWriteLockActive()) {
            return new DefaultKVMessage(msg.getKey(), null, KVMessage.StatusType.SERVER_WRITE_LOCK);
        }

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

    private AdminMessage handleAdminMessage(AdminMessage msg) {
        if (msg instanceof StartServerRequest) {
            serverState.setStopped(false);
            LOG.info("Admin: Started the server.");
            return GenericResponse.success();
        } else if (msg instanceof StopServerRequest) {
            serverState.setStopped(true);
            LOG.info("Admin: Stopped the server.");
            return GenericResponse.success();
        } else if (msg instanceof ShutDownServerRequest) {
            sessionRegistry.requestShutDown();
            LOG.info("Admin: Shutdown requested.");
            return GenericResponse.success();
        } else if (msg instanceof EnableWriteLockRequest) {
            serverState.setWriteLockActive(true);
            LOG.info("Admin: Enabled write lock.");
            return GenericResponse.success();
        } else if (msg instanceof DisableWriteLockRequest) {
            serverState.setWriteLockActive(false);
            LOG.info("Admin: Disabled write lock.");
            return GenericResponse.success();
        } else if (msg instanceof UpdateMetadataRequest) {
            UpdateMetadataRequest updateMetadataRequest = (UpdateMetadataRequest) msg;
            serverState.setClusterNodes(updateMetadataRequest.getNodes());
            LOG.info("Admin: Updated meta data.");
            return GenericResponse.success();
        } else if (msg instanceof MoveDataRequest) {
            MoveDataRequest moveDataRequest = (MoveDataRequest) msg;
            LOG.info("Admin: Starting move data task: {}", moveDataRequest);
            MoveDataTask moveDataTask = new MoveDataTask(
                    persistenceService, moveDataRequest.getRange(), moveDataRequest.getDestination());
            AdminTasks.addTask(moveDataTask);
            return GenericResponse.success();
        } else if (msg instanceof GetMaintenanceStatusRequest) {
            return new MaintenanceStatusResponse(AdminTasks.hasActiveTask(),
                    AdminTasks.getTaskType(),
                    (int) (100 * AdminTasks.getProgress()));
        } else {
            throw new AssertionError("Admin message handler not implemented: " + msg.getClass());
        }
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
