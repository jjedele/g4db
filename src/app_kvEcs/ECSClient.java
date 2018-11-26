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
            System.out.print("ECS Client> ");
            String input = reader.readLine();
            if (input != null && input.length() > 0) {
                try {
                    if (input.contains("init")) {
                        String [] result = input.split(" ");

                        int nrOfNodes = Integer.parseInt(result[1]);
                        int cacheSize = Integer.parseInt(result[2]);
                        CacheReplacementStrategy cacheReplacementStrategy = CacheReplacementStrategy.valueOf(result[3]);

                        client.initService(nrOfNodes, cacheSize, cacheReplacementStrategy);
                    } else if (input.contains("start")) {
                        client.start();
                    } else if (input.contains("stop")) {
                        client.stop();
                    } else if (input.contains("shutDown")) {
                        client.shutDown();
                    } else if (input.contains("removeNode")) {
                        client.removeNode();
                    } else if (input.contains("addNode")) {
                        String[] result = input.split(" ");

                        String arg2 = result[2];
                        int cacheSize = Integer.parseInt(arg2);

                        String arg3 = result[3];
                        CacheReplacementStrategy displacementStrategy = CacheReplacementStrategy.valueOf(arg3);

                        client.addNode(cacheSize, displacementStrategy);
                    } else if (input.contains("help")) {
                        System.out.println("Commands: start, stop, shutDown, removeNode, addNode");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }

}
