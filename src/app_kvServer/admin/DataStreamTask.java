package app_kvServer.admin;

import app_kvServer.ServerState;
import app_kvServer.persistence.PersistenceException;
import app_kvServer.persistence.PersistenceService;
import client.CommunicationModule;
import client.exceptions.ClientException;
import common.CorrelatedMessage;
import common.hash.HashRing;
import common.hash.Range;
import common.messages.DefaultKVMessage;
import common.messages.KVMessage;
import common.messages.admin.StreamCompleteMessage;
import common.utils.HostAndPort;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Task that moves data within a certain key range to another server.
 */
public class DataStreamTask implements AdminTask {

    private static final Logger LOG = LogManager.getLogger(DataStreamTask.class);

    private final String streamId;
    private final ServerState serverState;
    private final PersistenceService persistenceService;
    private final Range keyRange;
    private final HostAndPort destination;
    private Collection<String> keysToTransfer;
    private final AtomicInteger counter;

    private DataStreamTask(String streamId,
                          ServerState serverState,
                          PersistenceService persistenceService,
                          Collection<String> keysToTransfer,
                          Range keyRange,
                          HostAndPort destination) {
        this.streamId = streamId;
        this.serverState = serverState;
        this.persistenceService = persistenceService;
        this.keyRange = keyRange;
        this.destination = destination;
        this.keysToTransfer = keysToTransfer;
        this.counter = new AtomicInteger(0);
    }

    /**
     * Initialize a data stream task.
     * @param serverState State holder for the current node
     * @param persistenceService Persistence service
     * @param keyRange Range of keys to be transferred
     * @param destination Address of the server to which the data gets transferred
     * @return Stream task
     * @throws PersistenceException if keys can not be retrieved
     */
    public static DataStreamTask create(ServerState serverState,
                                        PersistenceService persistenceService,
                                        Range keyRange,
                                        HostAndPort destination) throws PersistenceException {
        String streamId = String.format("stream_%s_%d_%d_%d", serverState.getMyself().getHost(),
                serverState.getMyself().getPort(), keyRange.getStart(), keyRange.getEnd());

        // assemble a list of keys we want to transfer
        Collection<String> keysToTransfer = persistenceService.getKeys().stream()
                .filter(key -> keyRange.contains(HashRing.hash(key)))
                .collect(Collectors.toSet());

        return new DataStreamTask(streamId, serverState, persistenceService, keysToTransfer, keyRange, destination);
    }

    @Override
    public float getProgress() {
        if (keysToTransfer != null && keysToTransfer.size() == 0) {
            // no work
            return 1;
        } else {
            int divider = keysToTransfer != null ? keysToTransfer.size() : 1;
            return (float) counter.get() / divider;
        }
    }

    /**
     * Return the ID of this data stream.
     * @return
     */
    public String getStreamId() {
        return streamId;
    }

    /**
     * Return the number of total items to transfer.
     * @return
     */
    public int getNumberOfItemsToTransfer() {
        return keysToTransfer.size();
    }

    @Override
    public void run() {
        ThreadContext.put("serverPort", Integer.toString(serverState.getMyself().getPort()));
        CommunicationModule communicationModule = new CommunicationModule(destination);

        LOG.info("Enabling write lock because of data stream task: {}", streamId);
        serverState.setWriteLockActive(true);

        Instant start = Instant.now();
        try {
            communicationModule.start();

            // assemble a list of keys we want to transfer
            List<String> keysToTransfer = persistenceService.getKeys().stream()
                    .filter(key -> keyRange.contains(HashRing.hash(key)))
                    .collect(Collectors.toList());
            this.keysToTransfer = Collections.unmodifiableList(keysToTransfer);
            LOG.info("Starting transfer of {} entries to {}", keysToTransfer.size(), destination);

            List<CompletableFuture<KVMessage>> transfers = keysToTransfer.stream()
                    // create a PUT request for the entry
                    .map(key -> {
                        Optional<KVMessage> result = Optional.empty();
                        try {
                            result = persistenceService
                                    .get(key)
                                    .map(value -> new DefaultKVMessage(key, value, KVMessage.StatusType.PUT_REPLICA));
                        } catch (PersistenceException e) {
                            LOG.error("Error retrieving value.", e);
                        }
                        return result;
                    })
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    // send the PUT request to the destination server
                    .map(communicationModule::send)
                    // update the state of this task
                    .map(future -> future
                            .thenApply(CorrelatedMessage::getKVMessage)
                            .thenApply(reply -> {
                                int currentCount = counter.incrementAndGet();
                                if (currentCount % 100 == 0) {
                                    LOG.info("Stream {} progress: {}", streamId, getProgress());
                                }
                                if (KVMessage.StatusType.PUT_SUCCESS != reply.getStatus()
                                        && KVMessage.StatusType.PUT_UPDATE != reply.getStatus()) {
                                    LOG.warn("Could not transfer an item: {}", reply);
                                }
                                return reply;
                            }))
                    .collect(Collectors.toList());

            // wait until the whole transfer completed
            CompletableFuture<Void> overallTransfer = CompletableFuture.allOf(transfers.toArray(new CompletableFuture[] {}));
            overallTransfer.get();

            Instant end = Instant.now();
            try {
                CorrelatedMessage response = communicationModule
                        .send(new StreamCompleteMessage(streamId, keyRange))
                        .get(1, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                LOG.error("Could not send completion message for stream " + streamId, e);
            }
            LOG.info("Finished transfer of {} entries to {} in {}s",
                    keysToTransfer.size(), destination, Duration.between(start, end).getSeconds());
        } catch (PersistenceException | InterruptedException | ExecutionException | ClientException e) {
            LOG.error("Could not transfer values.", e);
        } finally {
            communicationModule.stop();
            LOG.info("Disabling write lock from data stream task: {}", streamId);
            serverState.setWriteLockActive(false);
        }
    }

}
