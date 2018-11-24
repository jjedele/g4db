package app_kvEcs;

import app_kvServer.CacheReplacementStrategy;
import client.KVAdminInterface;
import client.exceptions.ClientException;

import java.net.InetSocketAddress;

/**
 * External Configuration Service (ECS)
 */
public class ECSClient implements KVAdmin {

    private KVAdmin adminClient;
    private final int numberOfNodes;
    private final int cacheSize;
    private String cacheStrategy;

    public ECSClient(KVAdmin adminClient, int numberOfNodes, int cacheSize, String cacheStrategy) {
        this.adminClient = adminClient;
        this.numberOfNodes = numberOfNodes;
        this.cacheSize = cacheSize;
        this.cacheStrategy = cacheStrategy;
    }


    // TODO: Add "init <numberOfNodes> <cacheSize> <cacheStrategy>" command
    @Override
    public void initService(int numberOfNodes, int cacheSize, CacheReplacementStrategy displacementStrategy) {

    }

    // TODO: Add "start" command
    public void start() {
    }

    // TODO: Add "stop" command
    public void stop() {

    }

    // TODO: Add "shutDown" command
    public void shutDown() {

    }

    // TODO: Add "addNode <cacheSize> <cacheStrategy>" command
    @Override
    public void addNode(int cacheSize, CacheReplacementStrategy displacementStrategy) {

    }

    // TODO: Add "removeNode" command
    public void removeNode() {

    }


    /**
     * Start the REPL.
     *
     * @param args
     */
    public static void main(String[] args) throws ClientException, InterruptedException {
        KVAdminInterface adminClient = new client.KVAdmin(new InetSocketAddress("localhost", 50000));
        adminClient.connect();

        adminClient.shutDown();

        adminClient.disconnect();
    }

}
