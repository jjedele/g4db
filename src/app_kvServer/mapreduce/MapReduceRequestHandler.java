package app_kvServer.mapreduce;

import app_kvServer.gossip.Gossiper;
import app_kvServer.persistence.PersistenceService;
import common.messages.mapreduce.InitiateMRRequest;
import common.messages.mapreduce.InitiateMRResponse;
import common.messages.mapreduce.MRMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MapReduceRequestHandler {

    private final static Logger LOG = LogManager.getLogger(MapReduceRequestHandler.class);

    private final PersistenceService persistenceService;

    public MapReduceRequestHandler(PersistenceService persistenceService) {
        this.persistenceService = persistenceService;
    }

    public MRMessage handle(MRMessage request) {
        if (request instanceof InitiateMRRequest) {
            return handleInitiateMRRequest((InitiateMRRequest) request);
        } else {
            throw new IllegalStateException("Message type not supported here: " + request);
        }
    }

    public InitiateMRResponse handleInitiateMRRequest(InitiateMRRequest request) {
        // TODO
        LOG.warn("Starting map/reduce job with id={}", request.getId());
        LOG.warn(Gossiper.getInstance().getClusterDigest().getCluster());
        LOG.warn(request.getScript());

        InitiateMRResponse result = new InitiateMRResponse(request.getId(), null);
        return result;
    }

}
