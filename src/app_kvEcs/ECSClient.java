package app_kvEcs;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import app_kvServer.CacheReplacementStrategy;

/**
 * External Configuration Service (ECS)
 */
public class ECSClient {

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
            "addNode" + " <cacheSize> <cacheStrategy>" + "\n" +
            "exit - exit ECS";

    /**
     * Start the REPL.
     *
     * @param args
     */

    public static void main(String[] args) throws IOException {
        // read the config
        String workingDir = System.getProperty("user.dir");
        File configFilePath = new File(workingDir + "/ecs.config");
        if (!configFilePath.isFile()) {
            System.err.println("Need a ecs.config file in the working directory.");
        }
        Collection<DefaultKVAdmin.ServerInfo> servers = readEcsConfig(configFilePath);
        System.out.println("Available nodes: " + servers);

        DefaultKVAdmin kvAdmin = new DefaultKVAdmin(servers);
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

    private static Collection<DefaultKVAdmin.ServerInfo> readEcsConfig(File path) {
        DefaultKVAdmin.ServerInfo serverInfo;
        List<DefaultKVAdmin.ServerInfo> servers =new ArrayList<>();

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
            String line;

            while((line = bufferedReader.readLine()) != null) {
                // skip comments
                if (line.startsWith("#")) {
                    continue;
                }

                String[] parts = line.split(" ");

                String name = parts[0];
                String userName = parts[1];
                String host = parts[2];
                int port = Integer.parseInt(parts[3]);
                InetSocketAddress address = new InetSocketAddress(host, port);

                serverInfo = new DefaultKVAdmin.ServerInfo(name, userName, address);
                servers.add(serverInfo);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return servers;
    }

}
