package app_kvServer.mapreduce;

import app_kvServer.persistence.PersistenceService;
import common.messages.mapreduce.*;
import common.utils.HostAndPort;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.script.ScriptException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handler for map/reduce requests.
 */
public class MapReduceRequestHandler {

    private final static Logger LOG = LogManager.getLogger(MapReduceRequestHandler.class);

    private final HostAndPort myself;
    private final PersistenceService persistenceService;

    private final Map<String, MapReduceMaster> masters;

    /**
     * Constructor.
     *
     * @param myself Address of the local node.
     * @param persistenceService Persistence service.
     */
    public MapReduceRequestHandler(HostAndPort myself, PersistenceService persistenceService) {
        this.myself = myself;
        this.persistenceService = persistenceService;

        this.masters = new ConcurrentHashMap<>();
    }

    /**
     * Handle a {@link MRMessage}.
     *
     * @param request Request to handle.
     * @return Response to the request.
     */
    public MRMessage handle(MRMessage request) {
        if (request instanceof InitiateMRRequest) {
            return handleInitiateMRRequest((InitiateMRRequest) request);
        } else if (request instanceof ProcessingMRCompleteMessage) {
            return handleMRCompleteMessage((ProcessingMRCompleteMessage) request);
        } else if (request instanceof MRStatusRequest) {
            return handleMRStatusRequest((MRStatusRequest) request);
        } else {
            throw new IllegalStateException("Message type not supported here: " + request);
        }
    }

    private InitiateMRResponse handleInitiateMRRequest(InitiateMRRequest request) {
        boolean startMaster = request.getMaster() == null;

        if (startMaster) {
            try {
                MapReduceMaster master = new MapReduceMaster(myself, request);
                // FIXME this is a memory leak, must be get cleaned up when jobs finish
                masters.put(request.getId(), master);
                master.start();
            } catch (ScriptException e) {
                LOG.error("Could not start m/r master for job=" + request.getId(), e);
            }
        } else {
            MapReduceWorker worker = new MapReduceWorker(request, persistenceService);
            worker.start();
        }

        InitiateMRResponse result = new InitiateMRResponse(request.getId(), null);
        return result;
    }

    private ProcessingMRCompleteAcknowledgement handleMRCompleteMessage(ProcessingMRCompleteMessage request) {
        MapReduceMaster master = masters.get(request.getId());

        if (master != null) {
            master.collectResults(request.getRange(), request.getResults());
        } else {
            LOG.warn("Received data from completed m/r worker for non-existent master for job={}, range={}",
                    request.getId(), request.getRange());
        }

        return new ProcessingMRCompleteAcknowledgement();
    }

    private MRStatusMessage handleMRStatusRequest(MRStatusRequest request) {
        MapReduceMaster master = masters.get(request.getId());

        if (master == null) {
            return new MRStatusMessage(request.getId(), MRStatusMessage.Status.NOT_FOUND,
                    0, 0, 0, 0, null);
        } else {
            return master.getStatus();
        }
    }

}
