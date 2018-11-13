package app_kvEcs;

import client.KVAdminInterface;
import client.exceptions.ClientException;

import java.net.InetSocketAddress;

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
    public static void main(String[] args) throws ClientException, InterruptedException {
        KVAdminInterface adminClient = new client.KVAdmin(new InetSocketAddress("localhost", 50000));
        adminClient.connect();

        adminClient.shutDown();

        adminClient.disconnect();
    }

}
