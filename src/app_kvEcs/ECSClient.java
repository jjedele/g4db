package app_kvEcs;

import java.io.Console;
import java.util.Collections;
import java.util.Scanner;

import app_kvServer.CacheReplacementStrategy;

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
        adminClient.start();

    }

    // TODO: Add "stop" command
    public void stop() {
        adminClient.stop();

    }

    // TODO: Add "shutDown" command
    public void shutDown() {
        adminClient.shutDown();
    }

    // TODO: Add "addNode <cacheSize> <cacheStrategy>" command
    @Override
    public void addNode(int cacheSize, CacheReplacementStrategy displacementStrategy) {
        adminClient.addNode();
    }

    // TODO: Add "removeNode" command
    public void removeNode() {
        adminClient.removeNode();
    }

    /**
     * Start the REPL.
     *
     * @param args
     */
    public static void main(String[] args) {
        KVAdmin admin = new DefaultKVAdmin(Collections.emptyList());
        Scanner scanner = new Scanner(System.in).useDelimiter("\n");
        while (true) {
            System.out.println("Admin> ");
            String input = scanner.next();

            String[] parts = input.split(" ");

            if ("initCluster".equals(parts[0])) {
                if (parts[3] == "FIFO") {
                    admin.initService(Integer.parseInt(parts[1]), Integer.parseInt(parts[2]), CacheReplacementStrategy.FIFO);
                    System.out.println("Initializing the cluster.");
                } else if (parts[3] == "LRU") {
                    admin.initService(Integer.parseInt(parts[1]), Integer.parseInt(parts[2]), CacheReplacementStrategy.LRU);
                    System.out.println("Initializing the cluster.");
                } else {
                    admin.initService(Integer.parseInt(parts[1]), Integer.parseInt(parts[2]), CacheReplacementStrategy.LFU);
                    System.out.println("Initializing the cluster.");
                }
            }

            if ("start".equals(parts[0])) {
                admin.start();
                if (parts[3] == "FIFO") {
                    admin.initService(Integer.parseInt(parts[1]), Integer.parseInt(parts[2]), CacheReplacementStrategy.FIFO);
                    System.out.println("Start the cluster.");
                } else if (parts[3] == "LRU") {
                    admin.initService(Integer.parseInt(parts[1]), Integer.parseInt(parts[2]), CacheReplacementStrategy.LRU);
                    System.out.println("Start the cluster.");
                } else {
                    admin.initService(Integer.parseInt(parts[1]), Integer.parseInt(parts[2]), CacheReplacementStrategy.LFU);
                    System.out.println("Start the cluster.");
                }
            }

            if ("stop".equals(parts[0])) {
                admin.stop();
                if (parts[3] == "FIFO") {
                    admin.initService(Integer.parseInt(parts[1]), Integer.parseInt(parts[2]), CacheReplacementStrategy.FIFO);
                    System.out.println("stop the cluster.");
                } else if (parts[3] == "LRU") {
                    admin.initService(Integer.parseInt(parts[1]), Integer.parseInt(parts[2]), CacheReplacementStrategy.LRU);
                    System.out.println("stop the cluster.");
                } else {
                    admin.initService(Integer.parseInt(parts[1]), Integer.parseInt(parts[2]), CacheReplacementStrategy.LFU);
                    System.out.println("stop the cluster.");
                }
            }

            if ("shutDown()".equals(parts[0])) {
                admin.stop();
                if (parts[3] == "FIFO") {
                    admin.initService(Integer.parseInt(parts[1]), Integer.parseInt(parts[2]), CacheReplacementStrategy.FIFO);
                    System.out.println("stop the cluster.");
                } else if (parts[3] == "LRU") {
                    admin.initService(Integer.parseInt(parts[1]), Integer.parseInt(parts[2]), CacheReplacementStrategy.LRU);
                    System.out.println("stop the cluster.");
                } else {
                    admin.initService(Integer.parseInt(parts[1]), Integer.parseInt(parts[2]), CacheReplacementStrategy.LFU);
                    System.out.println("stop the cluster.");
                }
            }




        }

    }
}
