package app_kvServer.admin;

import app_kvServer.persistence.PersistenceException;
import app_kvServer.persistence.PersistenceService;
import client.CommunicationModule;
import common.CorrelatedMessage;
import common.hash.HashRing;
import common.hash.Range;
import common.messages.DefaultKVMessage;
import common.messages.KVMessage;
import common.messages.admin.MoveDataRequest;
import jdk.net.SocketFlow;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
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
    private final InetSocketAddress destination;
    private List<String> keysToTransfer;
    private final AtomicInteger counter;

    public MoveDataTask(PersistenceService persistenceService, Range keyRange, InetSocketAddress destination) {
        this.persistenceService = persistenceService;
        this.keyRange = keyRange;
        this.destination = destination;
        this.counter = new AtomicInteger(0);
    }

    @Override
    public float getProgress() {
        int divider = keysToTransfer != null ? keysToTransfer.size() : 1;
        return (float) counter.get() / divider;
    }

    @Override
    public void run() {
        CommunicationModule communicationModule = new CommunicationModule(destination, 1000);
        communicationModule.start();

        try {
            List<String> keysToTransfer = persistenceService.getKeys().stream()
                    .filter(key -> keyRange.contains(HashRing.hash(key)))
                    .collect(Collectors.toList());
            this.keysToTransfer = Collections.unmodifiableList(keysToTransfer);

            List<CompletableFuture<KVMessage>> transfers = keysToTransfer.stream()
                    .map(key -> {
                        Optional<KVMessage> result = Optional.empty();
                        try {
                            String value = persistenceService.get(key);
                            KVMessage putMessage = new DefaultKVMessage(key, value, KVMessage.StatusType.PUT);
                            result = Optional.of(putMessage);
                        } catch (PersistenceException e) {
                            LOG.error("Error retrieving value.", e);
                        }
                        return result;
                    })
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(communicationModule::send)
                    .map(future -> {
                        counter.incrementAndGet();
                        System.out.println(getProgress());
                        return future
                                .thenApply(CorrelatedMessage::getKVMessage)
                                .thenApply(reply -> {
                                    if (KVMessage.StatusType.PUT_SUCCESS != reply.getStatus()
                                            && KVMessage.StatusType.PUT_UPDATE != reply.getStatus()) {
                                        System.out.println(reply.getStatus());
                                    }
                                    return reply;
                                });
                    })
                    .collect(Collectors.toList());

            CompletableFuture<Void> overallTransfer = CompletableFuture.allOf(transfers.toArray(new CompletableFuture[] {}));
            overallTransfer.get();
        } catch (PersistenceException | InterruptedException | ExecutionException e) {
            LOG.error("Could not transfer values.", e);
        } finally {
            communicationModule.stop();
        }
    }

}
