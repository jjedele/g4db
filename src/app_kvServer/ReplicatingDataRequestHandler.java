package app_kvServer;

import app_kvServer.gossip.GossipEventListener;
import app_kvServer.persistence.PersistenceException;
import app_kvServer.persistence.PersistenceService;
import client.CommunicationModule;
import client.exceptions.ClientException;
import common.hash.HashRing;
import common.hash.NodeEntry;
import common.hash.Range;
import common.messages.DefaultKVMessage;
import common.messages.KVMessage;
import common.messages.gossip.ClusterDigest;
import common.messages.gossip.ServerState;
import common.utils.HostAndPort;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Handles data requests and replicates them according to the configured replication factor.
 */
public class ReplicatingDataRequestHandler implements DataRequestHandler, GossipEventListener {

    private final static Logger LOG = LogManager.getLogger(ReplicatingDataRequestHandler.class);
    private static final String KEY_NOT_FOUND = "Key not found";

    private final app_kvServer.ServerState serverState;
    private final HostAndPort myself;
    private final PersistenceService persistenceService;
    private int replicationFactor;
    private HashRing hashRing;
    private final CommunicationModule[] replicationConnections;

    /**
     * Constructor.
     * @param serverState Singleton server state holder
     * @param persistenceService Persistence service
     * @param replicationFactor How often each data item is replicated
     */
    public ReplicatingDataRequestHandler(app_kvServer.ServerState serverState,
                                         PersistenceService persistenceService,
                                         int replicationFactor) {
        this.serverState = serverState;
        this.myself = serverState.getMyself();
        this.persistenceService = persistenceService;
        this.replicationFactor = replicationFactor;
        this.replicationConnections = new CommunicationModule[replicationFactor - 1];
    }

    /**
     * Handle incoming data request.
     * @param msg request
     * @return reply
     */
    @Override
    public KVMessage handle(KVMessage msg) {
        KVMessage reply = null;
        switch (msg.getStatus()) {
            case PUT:
                reply = handlePutRequest(msg, false);
                break;
            case PUT_REPLICA:
                reply = handlePutRequest(msg, true);
                break;
            case GET:
                reply = handleGetRequest(msg);
                break;
            case DELETE:
                reply = handleDeleteRequest(msg, false);
                break;
            case DELETE_REPLICA:
                reply = handleDeleteRequest(msg, true);
                break;
            default:
                reply = new DefaultKVMessage("$SYS", "Invalid request", KVMessage.StatusType.PUT_ERROR);
        }

        return reply;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clusterChanged(ClusterDigest clusterDigest) {
        Set<HostAndPort> aliveNodes = clusterDigest.getCluster().entrySet().stream()
                .filter(e -> e.getValue().getStatus().isParticipating())
                .map(e -> e.getKey())
                .collect(Collectors.toSet());

        // we ourselves need to be in here when we're currently joining as well
        // otherwise we do not accept incoming data streams
        if (clusterDigest.getCluster().get(myself).getStatus() == ServerState.Status.JOINING) {
            aliveNodes.add(myself);
        }

        updatedNodes(aliveNodes);
    }

    private synchronized void updatedNodes(Collection<HostAndPort> nodes) {
        if (nodes.isEmpty()) {
            LOG.warn("Received empty node update. Skipping");
            return;
        }

        LOG.info("Updating nodes to {}", nodes);
        this.hashRing = new HashRing(nodes);

        for (int replicaNo = 1; replicaNo < replicationFactor; replicaNo++) {
            HostAndPort targetAddress = hashRing.getNthSuccessor(myself, replicaNo);
            CommunicationModule replicaConnection = replicationConnections[replicaNo - 1];
            if (replicaConnection == null || !targetAddress.equals(replicaConnection.getAddress())) {
                if (replicaConnection != null) {
                    LOG.info("Switching connection for replica {} from {} to {}",
                            replicaNo, replicaConnection.getAddress(), targetAddress);

                    replicaConnection.stop();
                } else {
                    LOG.info("Starting connection for replica {} to {}",
                            replicaNo, targetAddress);
                }

                try {
                    CommunicationModule newReplicaConnection = new CommunicationModule(targetAddress);
                    newReplicaConnection.start();
                    replicationConnections[replicaNo - 1] = newReplicaConnection;
                } catch (ClientException e) {
                    LOG.error("Could not start replica connection against " + targetAddress, e);
                }
            }
        }
    }

    private KVMessage handlePutRequest(KVMessage msg, boolean replicationRequest) {
        assert (msg.getStatus() == KVMessage.StatusType.PUT) || (msg.getStatus() == KVMessage.StatusType.PUT_REPLICA);

        // ensure state
        Optional<KVMessage> invalidStateReply = invalidStateReply(
                msg,
                true,
                !replicationRequest,
                true);
        if (invalidStateReply.isPresent()) {
            LOG.warn("Handled request={} for key={} with invalid state reply={}",
                    msg.getStatus(),
                    msg.getKey(),
                    invalidStateReply.get().getStatus());
            return invalidStateReply.get();
        }

        KVMessage reply;

        try {
            boolean insert = persistenceService.put(
                    msg.getKey(),
                    msg.getValue());

            KVMessage.StatusType status =
                    insert ? KVMessage.StatusType.PUT_SUCCESS : KVMessage.StatusType.PUT_UPDATE;

            reply = new DefaultKVMessage(
                    msg.getKey(),
                    null,
                    status);

            if (!replicationRequest) {
                replicatePut(msg.getKey(), msg.getValue());
            }

            LOG.info("Handled request={} for key={} with reply={}",
                    msg.getStatus(),
                    msg.getKey(),
                    reply.getStatus());
        } catch (PersistenceException e) {
            reply = new DefaultKVMessage(
                    msg.getKey(),
                    e.getMessage(),
                    KVMessage.StatusType.PUT_ERROR);

            LOG.warn(String.format("Handled request=%s for key=%s with error reply=%s",
                    msg.getStatus(),
                    msg.getKey(),
                    reply.getStatus()), e);
        }

        return reply;
    }

    private KVMessage handleGetRequest(KVMessage msg) {
        assert msg.getStatus() == KVMessage.StatusType.GET;

        // ensure state
        Optional<KVMessage> invalidStateReply = invalidStateReply(
                msg,
                true,
                true, // TODO should be false as soon we have replication
                true);
        if (invalidStateReply.isPresent()) {
            LOG.warn("Handled request={} for key={} with invalid state reply={}",
                    msg.getStatus(),
                    msg.getKey(),
                    invalidStateReply.get().getStatus());
            return invalidStateReply.get();
        }

        KVMessage reply;

        try {
            reply = persistenceService
                    .get(msg.getKey())
                    .map(value -> new DefaultKVMessage(msg.getKey(), value, KVMessage.StatusType.GET_SUCCESS))
                    .orElseThrow(() -> new IllegalArgumentException(KEY_NOT_FOUND));

            LOG.info("Handled request={} for key={} with reply={}",
                    msg.getStatus(),
                    msg.getKey(),
                    reply.getStatus());
        } catch (IllegalArgumentException e) {
            reply = new DefaultKVMessage(
                    msg.getKey(),
                    e.getMessage(),
                    KVMessage.StatusType.GET_ERROR);

            LOG.info("Handled request={} for key={} with " + KEY_NOT_FOUND,
                    msg.getStatus(),
                    msg.getKey());
        } catch (PersistenceException e) {
            reply = new DefaultKVMessage(
                    msg.getKey(),
                    e.getMessage(),
                    KVMessage.StatusType.GET_ERROR);

            LOG.warn(String.format("Handled request=%s for key=%s with error reply=%s",
                    msg.getStatus(),
                    msg.getKey(),
                    reply.getStatus()), e);
        }

        return reply;
    }

    private KVMessage handleDeleteRequest(KVMessage msg, boolean replicationRequest) {
        assert (msg.getStatus() == KVMessage.StatusType.DELETE)
                || (msg.getStatus() == KVMessage.StatusType.DELETE_REPLICA);

        // ensure state
        Optional<KVMessage> invalidStateReply = invalidStateReply(
                msg,
                true,
                !replicationRequest,
                true);
        if (invalidStateReply.isPresent()) {
            LOG.warn("Handled request={} for key={} with invalid state reply={}",
                    msg.getStatus(),
                    msg.getKey(),
                    invalidStateReply.get().getStatus());
            return invalidStateReply.get();
        }

        KVMessage reply;

        try {
            boolean deleted = persistenceService.delete(msg.getKey());

            if (deleted) {
                reply = new DefaultKVMessage(msg.getKey(),
                        msg.getValue(),
                        KVMessage.StatusType.DELETE_SUCCESS);

                LOG.info("Handled request={} for key={} with reply={}",
                        msg.getStatus(),
                        msg.getKey(),
                        reply.getStatus());
            } else {
                reply = new DefaultKVMessage(msg.getKey(),
                        KEY_NOT_FOUND,
                        KVMessage.StatusType.DELETE_ERROR);

                LOG.info("Handled request={} for key={} with " + KEY_NOT_FOUND,
                        msg.getStatus(),
                        msg.getKey());
            }

            if (!replicationRequest) {
                replicateDelete(msg.getKey());
            }

        } catch (PersistenceException e) {
            reply = new DefaultKVMessage(
                    msg.getKey(),
                    e.getMessage(),
                    KVMessage.StatusType.DELETE_ERROR);

            LOG.warn(String.format("Handled request=%s for key=%s with error reply=%s",
                    msg.getStatus(),
                    msg.getKey(),
                    reply.getStatus()), e);
        }

        return reply;
    }

    private Optional<KVMessage> invalidStateReply(KVMessage msg,
                                                  boolean ensureServerRunning,
                                                  boolean ensurePrimaryResponsibility,
                                                  boolean ensureReplicaResponsibility) {
        LOG.traceEntry("state", msg, ensureServerRunning, ensurePrimaryResponsibility, ensureReplicaResponsibility);

        Optional<KVMessage> reply = Optional.empty();
        if (ensureServerRunning && serverState.isStopped()) {
            LOG.trace("Server is stopped.");
            reply = Optional.of(new DefaultKVMessage(msg.getKey(), null, KVMessage.StatusType.SERVER_STOPPED));

            LOG.traceExit(reply);
            return reply;
        }

        int replicaNumber = getReplicaNumber(msg.getKey());
        boolean msgInPrimaryResponsibility = replicaNumber == 0;
        boolean msgInReplicaResponsibility = replicaNumber < replicationFactor;

        if ((ensurePrimaryResponsibility && !msgInPrimaryResponsibility)
            || (ensureReplicaResponsibility && !msgInReplicaResponsibility)) {
            LOG.warn("Rejecting key={} with hash={} which would go to pr={} on ring {}", msg.getKey(),
                    HashRing.hash(msg.getKey()), hashRing.getResponsibleNode(msg.getKey()), hashRing);
            LOG.trace("Primary responsibility: {}, Replica responsibility: {}",
                    msgInPrimaryResponsibility,
                    msgInReplicaResponsibility);

            List<NodeEntry> nodes = hashRing.getNodes().stream()
                    .map(address -> new NodeEntry("", address, new Range(0, 1)))
                    .collect(Collectors.toList());
            // TODO remove cluster nodes
            reply = Optional.of(new DefaultKVMessage(
                    msg.getKey(),
                    NodeEntry.multipleToSerializableString(nodes),
                    KVMessage.StatusType.SERVER_NOT_RESPONSIBLE));
        }

        LOG.traceExit(reply);
        return reply;
    }


    private synchronized int getReplicaNumber(String key) {
        return hashRing.findSuccessorNumber(hashRing.getResponsibleNode(key), myself);
    }

    private synchronized void replicatePut(String key, String value) {
        for (int replicaNo = 1; replicaNo < replicationFactor; replicaNo++) {
            Optional<CommunicationModule> replicationConnection =
                    Optional.ofNullable(replicationConnections[replicaNo - 1]);

            if (!replicationConnection.isPresent()) {
                LOG.warn("Could not replicate request=PUT for key={} because not connection to replica={}",
                        key, replicaNo);
                continue;
            }

            replicationConnection.ifPresent(connection -> {
                connection.send(new DefaultKVMessage(key, value, KVMessage.StatusType.PUT_REPLICA));
                LOG.info("Replicated request=PUT for key={} to target={}", key, connection.getAddress());
            });
        }
    }

    private synchronized void replicateDelete(String key) {
        for (int replicaNo = 1; replicaNo < replicationFactor; replicaNo++) {
            Optional<CommunicationModule> replicationConnection =
                    Optional.ofNullable(replicationConnections[replicaNo - 1]);

            if (!replicationConnection.isPresent()) {
                LOG.warn("Could not replicate request=DELETE for key={} because not connection to replica={}",
                        key, replicaNo);
                continue;
            }

            replicationConnection.ifPresent(connection -> {
                connection.send(new DefaultKVMessage(key, null, KVMessage.StatusType.DELETE_REPLICA));
                LOG.info("Replicated request=DELETE for key={} to target={}", key, connection.getAddress());
            });
        }
    }

}
