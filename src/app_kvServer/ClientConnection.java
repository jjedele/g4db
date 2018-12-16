package app_kvServer;

import app_kvServer.admin.AdminTasks;
import app_kvServer.admin.CleanUpDataTask;
import app_kvServer.admin.DataStreamTask;
import app_kvServer.admin.MoveDataTask;
import app_kvServer.gossip.Gossiper;
import app_kvServer.persistence.PersistenceService;
import app_kvServer.sync.Synchronizer;
import common.CorrelatedMessage;
import common.Protocol;
import common.exceptions.ProtocolException;
import common.hash.Range;
import common.messages.ExceptionMessage;
import common.messages.Message;
import common.messages.admin.*;
import common.messages.gossip.ClusterDigest;
import common.utils.ContextPreservingThread;
import common.utils.RecordReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A ClientConnection represents an active session with a client application.
 */
public class ClientConnection extends ContextPreservingThread {

    private static final Logger LOG = LogManager.getLogger(ClientConnection.class);

    private static final byte RECORD_SEPARATOR = 0x1e;

    private final AtomicBoolean running;
    private final PersistenceService persistenceService;
    private final DataRequestHandler dataRequestHandler;
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
                            ServerState serverState,
                            DataRequestHandler dataRequestHandler) {
        this.socket = clientSocket;
        this.running = new AtomicBoolean(false);
        this.persistenceService = persistenceService;
        this.sessionRegistry = sessionRegistry;
        this.serverState = serverState;
        this.dataRequestHandler = dataRequestHandler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        this.setUpThreadContext();
        try {
            // TODO handle errors more gracefully
            this.running.set(true);
            sessionRegistry.registerSession(this);

            outputStream = socket.getOutputStream();
            inputStream = socket.getInputStream();
            RecordReader recordReader = new RecordReader(inputStream, RECORD_SEPARATOR);

            ThreadContext.put("client", socket.getRemoteSocketAddress().toString());
            LOG.debug("Established connection with client.");

            while (running.get()) {
                byte[] incoming = recordReader.read();

                if (incoming == null) {
                    LOG.debug("Terminating based on client's request.");
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
                } catch (Exception e) {
                    LOG.error("Protocol exception.", e);
                    response = new ExceptionMessage(e);
                }

                byte[] outgoing = Protocol.encode(response, correlationNumber);
                outputStream.write(outgoing);
                outputStream.write(RECORD_SEPARATOR);
                outputStream.flush();

                ThreadContext.remove("correlation");
            }

            outputStream.write(Protocol.SHUTDOWN_CMD);
            outputStream.write(RECORD_SEPARATOR);
            outputStream.flush();

            socket.close();
        } catch (SocketException e) {
            // happens when the connection closes, we don't want to flood the log with errors because of that
            LOG.debug("Socket exception", e);
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
            return dataRequestHandler.handle(request.getKVMessage());
        } else if (request.hasAdminMessage()) {
            return handleAdminMessage(request.getAdminMessage());
        } else if (request.hasGossipMessage()) {
            ClusterDigest incomingDigest = (ClusterDigest) request.getGossipMessage();
            return Optional.ofNullable(incomingDigest)
                    .map(Gossiper.getInstance()::handleIncomingDigest)
                    .orElse(Gossiper.getInstance().getClusterDigest());
        } else {
            throw new ProtocolException("Unsupported request: " + request);
        }
    }

    private AdminMessage handleAdminMessage(AdminMessage msg) {
        if (msg instanceof StartServerRequest) {
            StartServerRequest startServerRequest = (StartServerRequest) msg;
            if (startServerRequest.isClusterInit()) {
                Gossiper.getInstance().setOwnState(common.messages.gossip.ServerState.Status.OK);
            } else {
                Synchronizer.getInstance().initiateJoin();
            }
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

            Range newKeyRange = serverState.getClusterNodes().getAssignedRange(serverState.getMyself());
            CleanUpDataTask cleanUpDataTask = new CleanUpDataTask(persistenceService, newKeyRange);
            AdminTasks.addTask(cleanUpDataTask);

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
        } else if (msg instanceof InitiateStreamRequest) {
            InitiateStreamRequest req = (InitiateStreamRequest) msg;
            LOG.info("Admin: Initiate stream request for range {}", req.getKeyRange());
            // TODO check cluster state
            try {
                DataStreamTask streamTask = DataStreamTask.create(serverState, persistenceService, req.getKeyRange(),
                        req.getDestination());
                AdminTasks.addTask(streamTask);
                LOG.info("Admin: Initiated stream request: ", streamTask.getStreamId());
                return new InitiateStreamResponse(true, streamTask.getStreamId(),
                        streamTask.getNumberOfItemsToTransfer(), null);
            } catch (Exception e) {
                LOG.error("Could not instantiate data stream.", e);
                return new InitiateStreamResponse(false, null,
                        0, null);
            }
        } else if (msg instanceof StreamCompleteMessage) {
            StreamCompleteMessage req = (StreamCompleteMessage) msg;
            Synchronizer.getInstance().streamCompleted(req.getRange());
            return GenericResponse.success();
        } else {
            throw new AssertionError("Admin message handler not implemented: " + msg.getClass());
        }
    }

    private void cleanConnectionShutdown() {
        LOG.debug("Closing connection.");
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException e) {
                LOG.warn("Error closing connection.", e);
            }
        }
        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException e) {
                LOG.warn("Error closing connection.", e);
            }
        }
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                LOG.warn("Error closing connection.", e);
            }
        }
        running.set(false);
    }

}
