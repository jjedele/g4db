package app_kvEcs;

import java.io.Console;
import java.util.Collections;
import java.util.Scanner;

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
        KVAdmin admin = new DefaultKVAdmin(Collections.emptyList());
        Scanner scanner = new Scanner(System.in).useDelimiter("\n");
        while (true) {
            System.out.println("Admin> ");
            String input = scanner.next();

            String[] parts = input.split(" ");

            if ("initCluster".equals(parts[0])) {
                int numberOfNodes = Integer.parseInt(parts[1]);
                int cacheSize = Integer.parseInt(parts[2]);
                CacheReplacementStrategy cacheReplacementStrategy = CacheReplacementStrategy.valueOf(parts[3]);
                admin.initService(numberOfNodes, cacheSize, cacheReplacementStrategy);
                System.out.println("Initializing the cluster.");
            }

            if ("start".equals(parts[0])) {
                admin.start();

            }

            if ("stop".equals(parts[0])) {
                admin.stop();

            }

            if ("shutDown()".equals(parts[0])) {
                admin.shutDown();

            }
            if ("addNode()".equals(parts[0])) {
                int cacheSize = Integer.parseInt(parts[1]);
                CacheReplacementStrategy displacementStrategy = CacheReplacementStrategy.valueOf(parts[2]);
                admin.addNode(cacheSize, displacementStrategy);
            }

            if ("removeNode()".equals(parts[0])){
                admin.removeNode();
            }

            /*
            KVAdminInterface adminClient = new client.KVAdmin(new InetSocketAddress("localhost", 50000));
            adminClient.connect();

            adminClient.shutDown();

            adminClient.disconnect();
            */

        }

    }
}
