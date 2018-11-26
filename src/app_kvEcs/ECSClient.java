package app_kvEcs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import app_kvServer.CacheReplacementStrategy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * External Configuration Service (ECS)
 */
public class ECSClient {

    private static final Logger LOG = LogManager.getLogger(ECSClient.class);

    private static final String initCommand = "init";
    private static final String startCommand = "start";
    private static final String stopCommand = "stop";
    private static final String shutDownCommand = "shutDown";
    private static final String addNodeCommand = "addNode";
    private static final String removeNodeCommand = "removeNode";
    private static final String exitCommand = "exit";
    private static final String helpCommand = "Commands: \n" +
            "init <numberOfNodes> <cacheSize> <cacheStrategy>" + "\n" +
            "start" + " - start the cluster" + "\n" +
            "stop" + " - stop the cluster" + "\n" +
            "shutDown" + " - shut down the cluster " + "\n" +
            "removeNode" + " - remove a node form the cluster" + "\n" +
            "addNode" + " <cacheSize> <cacheStrategy>";

    /**
     * Start the REPL.
     *
     * @param args
     */

    public static void main(String[] args) throws IOException {
        DefaultKVAdmin kvAdmin = new DefaultKVAdmin();
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        System.out.println(helpCommand);

        while (true) {
            System.out.print("ECS Client> ");
            String input = reader.readLine();
            if (input != null && input.length() > 0) {
                try {
                    if (input.contains(initCommand)) {
                        String[] result = input.split(" ");
                        int nrOfNodes = Integer.parseInt(result[1]);

                        int cacheSize = Integer.parseInt(result[2]);
                        CacheReplacementStrategy cacheReplacementStrategy = CacheReplacementStrategy.valueOf(result[3]);

                        kvAdmin.initService(nrOfNodes, cacheSize, cacheReplacementStrategy);
                    } else if (input.contains(startCommand)) {
                        kvAdmin.start();
                    } else if (input.contains(stopCommand)) {
                        kvAdmin.stop();
                    } else if (input.contains(shutDownCommand)) {
                        kvAdmin.shutDown();
                    } else if (input.contains(removeNodeCommand)) {
                        kvAdmin.removeNode();
                    } else if (input.contains(addNodeCommand)) {
                        String[] result = input.split(" ");
                        int cacheSize = Integer.parseInt(result[1]);
                        CacheReplacementStrategy displacementStrategy = CacheReplacementStrategy.valueOf(result[2]);
                        kvAdmin.addNode(cacheSize, displacementStrategy);
                    } else if (input.contains(exitCommand)) {
                        System.exit(1);
                    } else {
                        System.out.println("Wrong command");
                        System.out.println(helpCommand);
                    }
                } catch (ArrayIndexOutOfBoundsException e) {
                    System.out.println("Wrong number of arguments" + "\n" + helpCommand);
                } catch (RuntimeException e) {
                    System.out.println(e.getMessage());
                    System.out.println(helpCommand);
                }
            }
        }

    }

}
