package app_kvServer.admin;

import app_kvServer.persistence.PersistenceException;
import app_kvServer.persistence.PersistenceService;
import client.CommunicationModule;
import client.exceptions.ClientException;
import common.CorrelatedMessage;
import common.hash.HashRing;
import common.hash.Range;
import common.messages.DefaultKVMessage;
import common.messages.KVMessage;
import common.utils.HostAndPort;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Task that moves data within a certain key range to another server.
 */
public class MoveDataTask implements AdminTask {

    private static final Logger LOG = LogManager.getLogger(MoveDataTask.class);

    private final PersistenceService persistenceService;
    private final Range keyRange;
    private final HostAndPort destination;
    private List<String> keysToTransfer;
    private final AtomicInteger counter;

    /**
     * Constructor.
     * @param persistenceService Persistence service to get entries from
     * @param keyRange Range of keys to transfer
     * @param destination Destination server
     */
    public MoveDataTask(PersistenceService persistenceService, Range keyRange, HostAndPort destination) {
        this.persistenceService = persistenceService;
        this.keyRange = keyRange;
        this.destination = destination;
        this.counter = new AtomicInteger(0);
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

    @Override
    public void run() {
        CommunicationModule communicationModule = new CommunicationModule(destination, 1000);

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
                                    .map(value -> new DefaultKVMessage(key, value, KVMessage.StatusType.PUT));
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
                                counter.incrementAndGet();
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
            LOG.info("Finished transfer of {} entries to {}", keysToTransfer.size(), destination);
        } catch (PersistenceException | InterruptedException | ExecutionException | ClientException e) {
            LOG.error("Could not transfer values.", e);
        } finally {
            communicationModule.stop();
        }
    }

}
