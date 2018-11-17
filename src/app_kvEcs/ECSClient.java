package app_kvEcs;

import java.io.Console;
import java.util.Collections;
import java.util.Scanner;

/**
 * External Configuration Service (ECS)
 */
public class ECSClient {

    private KVAdmin adminClient;

    // TODO: Add "init <numberOfNodes> <cacheSize> <cacheStrategy>" command
    // TODO: Add "start" command
    // TODO: Add "stop" command
    // TODO: Add "shutDown" command
    // TODO: Add "addNode <cacheSize> <cacheStrategy>" command
    // TODO: Add "removeNode" command

    /**
     * Start the REPL.
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
                // TODO　admin.initService();
                System.out.println("Initializing the cluster.");
            }
        }
    }

}
