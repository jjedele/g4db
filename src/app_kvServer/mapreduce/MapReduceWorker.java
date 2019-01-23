package app_kvServer.mapreduce;

import app_kvServer.persistence.PersistenceException;
import app_kvServer.persistence.PersistenceService;
import client.CommunicationModule;
import client.exceptions.ClientException;
import common.hash.HashRing;
import common.messages.mapreduce.InitiateMRRequest;
import common.messages.mapreduce.ProcessingMRCompleteMessage;
import common.utils.ContextPreservingThread;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import javax.script.ScriptException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Map/reduce worker process.
 *
 * Does the actual data processing.
 */
public class MapReduceWorker extends ContextPreservingThread {

    private final static Logger LOG = LogManager.getLogger(MapReduceWorker.class);

    private final InitiateMRRequest request;
    private final PersistenceService persistenceService;
    private final AtomicBoolean running;
    private volatile Throwable error;

    /**
     * Constructor.
     *
     * @param request Request that triggered the map/reduce process.
     * @param persistenceService Persistence service.
     */
    public MapReduceWorker(InitiateMRRequest request, PersistenceService persistenceService) {
        this.request = request;
        this.persistenceService = persistenceService;
        this.running = new AtomicBoolean(false);
        this.error = null;
    }

    /**
     * Run the map/reduce process.
     */
    @Override
    public void run() {
        setUpThreadContext();
        ThreadContext.put("mrJob", request.getId());
        LOG.info("Starting map/reduce worker process with id={}",
                request.getId(), request.getSourceNamespace());

        running.set(true);

        try {
            MapReduceProcessor processor = new MapReduceProcessor(request.getScript());

            // FIXME default NS "." should only be known to PersistenceService
            String sourceNamespace = Optional.ofNullable(request.getSourceNamespace()).orElse(".");
            List<String> keys = persistenceService.getKeys(sourceNamespace).stream()
                    .filter(key -> request.getSourceKeyRange().contains(HashRing.hash(key)))
                    .collect(Collectors.toList());
            LOG.info("Map/reduce on src_ns={} for range={} processing n_keys={}",
                    request.getSourceNamespace(), request.getSourceKeyRange(), keys.size());

            for (String key : keys) {
                try {
                    String value = persistenceService
                            .get(key)
                            .orElseThrow(() -> new PersistenceException("Key does not exist anymore: " + key));

                    processor.process(key, value);
                } catch (PersistenceException e) {
                    LOG.warn("Could not map/reduce key=" + key, e);
                }
            }

            // send results back to the master
            LOG.info("Sending map/reduce results back to master={}", request.getMaster());
            ProcessingMRCompleteMessage completeMessage = new ProcessingMRCompleteMessage(
                    request.getId(), request.getSourceKeyRange(), processor.getResults());

            CommunicationModule communicationModule = new CommunicationModule(request.getMaster());
            communicationModule.start();

            communicationModule
                    .send(completeMessage)
                    .whenComplete((res, exc) -> communicationModule.stop())
                    .get(60, TimeUnit.SECONDS);
        } catch (ScriptException | PersistenceException e) {
            LOG.error("Could not run map/reduce.", e);
            error = e;
        } catch (ClientException | InterruptedException | ExecutionException | TimeoutException e) {
            LOG.error("Could not send map/reduce results back to master.", e);
            error = e;
        } catch (Exception e) {
            error = e;
            throw e;
        } finally {
            running.set(false);
        }
    }

    /**
     * Return if the worker is currently running.
     *
     * @return Running status
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Return the error condition of this worker or null if there is none.
     *
     * @return
     */
    public Throwable getError() {
        return error;
    }
}
