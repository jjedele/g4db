package app_kvEcs;

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import app_kvClient.KVClient;
import app_kvClient.commands.*;
import app_kvEcs.DefaultKVAdmin.ServerInfo;
import app_kvServer.CacheReplacementStrategy;
import client.KVAdminInterface;
import client.exceptions.ClientException;
import com.jcraft.jsch.JSchException;

import java.net.InetSocketAddress;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.swing.text.html.Option;

/**
 * External Configuration Service (ECS)
 */
public class ECSClient implements KVAdmin {

    private static final Logger LOG = LogManager.getLogger(ECSClient.class);
    private final HashMap<String, Class<? extends Command>> commands;

    private DefaultKVAdmin defaultKVAdmin;
    private CacheReplacementStrategy cacheStrategy;
    InetSocketAddress address;
    private boolean exiting;

    public ECSClient() {
        this.defaultKVAdmin = new DefaultKVAdmin(Arrays.asList(
                new ServerInfo("node1", "xhens", new InetSocketAddress("localhost", 50000)),
                new ServerInfo("node2", "xhens", new InetSocketAddress("localhost", 50001)),
                new ServerInfo("node3", "xhens", new InetSocketAddress("localhost", 50002)),
                new ServerInfo("node4", "xhens", new InetSocketAddress("localhost", 50003))
        ));
        this.commands = new HashMap<>();
        // this.commands.put();
    }


    // TODO: Add "init <numberOfNodes> <cacheSize> <cacheStrategy>" command
    @Override
    public void initService(int numberOfNodes, int cacheSize, CacheReplacementStrategy displacementStrategy) {
        try {
            defaultKVAdmin.initService(numberOfNodes, cacheSize, displacementStrategy);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // TODO: Add "start" command
    public void start() {
        defaultKVAdmin.start();
    }

    // TODO: Add "stop" command
    public void stop() {
        defaultKVAdmin.stop();
    }

    // TODO: Add "shutDown" command
    public void shutDown() {
        defaultKVAdmin.shutDown();
    }

    // TODO: Add "addNode <cacheSize> <cacheStrategy>" command
    @Override
    public void addNode(int cacheSize, CacheReplacementStrategy displacementStrategy) {
        defaultKVAdmin.addNode(cacheSize, displacementStrategy);
    }

    // TODO: Add "removeNode" command
    public void removeNode() {
        defaultKVAdmin.removeNode();
    }

    public boolean isExiting() {
        return exiting;
    }

    /*
    public Optional parseInput(final String input) {
        String[] parts = KVClient(input);
        String name = parts[0];
        String[] arguments = Arrays.copyOfRange(parts, 1, parts.length);

        if (!this.commands.containsKey(name)) {
            System.out.println("No such command");
            return Optional.empty();
        }
        return Optional.of(arguments);
    }
    */

    // TODO add isConnected after everything is done
    /*
    public String getPrompt() {
        StringBuilder builder = new StringBuilder("EchoClient");
        if (defaultKVAdmin.isConnected()) {
            builder.append(" (connected)");
        }
        builder.append("> ");
        return builder.toString();
    }
    */


    /**
     * Start the REPL.
     *
     * @param args
     */

    // TODO read config file to init the servers
    public static void main(String[] args) throws ClientException, InterruptedException, IOException {
        ECSClient client = new ECSClient();
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        while (!client.isExiting()) {
            String input = reader.readLine();
            System.out.println(input);
            if (input != null && input.length() > 0) {
                try {
                    if (input.contains("init")) {
                        client.initService(4, 50, CacheReplacementStrategy.FIFO);
                    } else if (input.contains("start")) {
                        client.start();
                    } else if (input.contains("stop")) {
                        client.stop();
                    } else if (input.contains("shutdown")) {
                        client.shutDown();
                    } else if (input.contains("remove node")) {
                        client.removeNode();
                    } else if (input.contains("add node")) {
                        String[] result = input.split(" ");

                        String arg2 = result[2];
                        int cacheSize = Integer.parseInt(arg2);

                        String arg3 = result[3];
                        CacheReplacementStrategy displacementStrategy = CacheReplacementStrategy.valueOf(arg3);

                        client.addNode(cacheSize, displacementStrategy);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        /*
        Scanner scanner = new Scanner(System.in).useDelimiter("\n");
        while (true) {
            System.out.println("Admin> ");
            String input = scanner.next();

            String[] parts = input.split(" ");

            if ("initCluster".equals(parts[0])) {
                int numberOfNodes = Integer.parseInt(parts[1]);
                int cacheSize = Integer.parseInt(parts[2]);
                CacheReplacementStrategy cacheReplacementStrategy = CacheReplacementStrategy.valueOf(parts[3]);
                kvAdmin.initService(numberOfNodes, cacheSize, cacheReplacementStrategy);
                System.out.println("Initializing the cluster.");
            }

            if ("start".equals(parts[0])) {
                kvAdmin.start();
            }

            if ("stop".equals(parts[0])) {
                kvAdmin.stop();

            }

            if ("shutDown()".equals(parts[0])) {
                kvAdmin.shutDown();

            }
            if ("addNode()".equals(parts[0])) {
                int cacheSize = Integer.parseInt(parts[1]);
                CacheReplacementStrategy displacementStrategy = CacheReplacementStrategy.valueOf(parts[2]);
                kvAdmin.addNode(cacheSize, displacementStrategy);
            }

            if ("removeNode()".equals(parts[0])) {
                kvAdmin.removeNode();
            }


            /*
            KVAdminInterface adminClient = new client.KVAdmin(new InetSocketAddress("localhost", 50000));
            adminClient.connect();

            adminClient.shutDown();

            adminClient.disconnect();
            */
    }

}
