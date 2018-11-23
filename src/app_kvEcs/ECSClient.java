package app_kvEcs;

import java.io.Console;
import java.util.Collections;
import java.util.Scanner;

import app_kvServer.CacheReplacementStrategy;

/**
 * External Configuration Service (ECS)
 */
public class ECSClient {

    private KVAdmin adminClient;

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


        }

    }
}
