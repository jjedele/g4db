package testing;

import app_kvServer.CacheReplacementStrategy;
import app_kvServer.KVServer;
import client.KVAdmin;
import client.KVStore;
import client.exceptions.ClientException;
import common.hash.NodeEntry;
import common.hash.Range;
import common.messages.KVMessage;
import common.messages.admin.GenericResponse;
import common.messages.admin.UpdateMetadataRequest;
import junit.framework.TestCase;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Random;

public class ServerLifecycleTest extends TestCase {

    private int port;
    private KVServer server;
    private File dataDir;
    private Thread serverThread;
    private KVStore kvClient;
    private KVAdmin kvAdmin;

    @Override
    protected void setUp() throws Exception {
        // server
        System.setProperty("hash_test_mode", "true");
        port = new Random().nextInt(10000) + 10000;
        dataDir = new File(System.getProperty("java.io.tmpdir"), "servertest-" + System.currentTimeMillis());
        server = new KVServer(port, dataDir, 100, CacheReplacementStrategy.FIFO);
        serverThread = new Thread(server);
        serverThread.start();

        // client
        kvClient = new KVStore("localhost", port);
        kvClient.connect();

        // admin client
        kvAdmin = new KVAdmin(new InetSocketAddress("localhost", port));
        kvAdmin.connect();
    }

    @Override
    protected void tearDown() throws Exception {
        // client
        kvClient.disconnect();

        // admin client
        kvAdmin.disconnect();

        // server
        server.stop();
        serverThread.join(10000);
        super.tearDown();
    }

    public void testNoRequestsAcceptedUntilServerStarted() throws Exception {
        KVMessage reply = kvClient.put("foo", "bar");

        assertEquals(KVMessage.StatusType.SERVER_STOPPED, reply.getStatus());

        startServer();

        reply = kvClient.put("foo", "bar");

        assertEquals(KVMessage.StatusType.PUT_SUCCESS, reply.getStatus());

        stopServer();

        reply = kvClient.get("foo");

        assertEquals(KVMessage.StatusType.SERVER_STOPPED, reply.getStatus());
    }

    public void testWriteLock() throws Exception {
        startServer();

        KVMessage reply = kvClient.put("foo", "bar");
        assertEquals(KVMessage.StatusType.PUT_SUCCESS, reply.getStatus());

        enableWriteLock();

        reply = kvClient.put("foo", "bar");
        assertEquals(KVMessage.StatusType.SERVER_WRITE_LOCK, reply.getStatus());

        reply = kvClient.get("foo");
        assertEquals(KVMessage.StatusType.GET_SUCCESS, reply.getStatus());
        assertEquals("bar", reply.getValue());

        disableWriteLock();

        reply = kvClient.put("foo", "bar");
        assertEquals(KVMessage.StatusType.PUT_UPDATE, reply.getStatus());
    }

    public void testKeyRangeResponsibility() throws Exception {
        startServer();

        String key1 = "foo";
        int key1Hash = key1.getBytes()[0];
        String key2 = "bar";
        int key2Hash = key2.getBytes()[0];

        KVMessage reply = kvClient.put(key1, "baz");
        assertEquals(KVMessage.StatusType.PUT_SUCCESS, reply.getStatus());
        reply = kvClient.put(key2, "baz");
        assertEquals(KVMessage.StatusType.PUT_SUCCESS, reply.getStatus());

        // assign key ranges such that server is responsible for key2 but not key1
        NodeEntry ourNode = new NodeEntry(
                "us",
                new InetSocketAddress("localhost", port),
                new Range(0, key2Hash));
        NodeEntry imaginaryNode = new NodeEntry(
                "us",
                new InetSocketAddress("localhost", port + 42),
                new Range(key2Hash, key1Hash));

        GenericResponse updateReply = kvAdmin.updateMetadata(Arrays.asList(ourNode, imaginaryNode));
        assertTrue(updateReply.getMessage(), updateReply.isSuccess());

        reply = kvClient.get(key2);
        assertEquals(KVMessage.StatusType.GET_SUCCESS, reply.getStatus());

        reply = kvClient.get(key1);
        assertEquals(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE, reply.getStatus());
        assertEquals(NodeEntry.multipleToSerializableString(Arrays.asList(ourNode, imaginaryNode)), reply.getValue());
    }

    private void startServer() throws ClientException {
        GenericResponse reponse = kvAdmin.start();
        assertTrue(reponse.getMessage(), reponse.isSuccess());
    }

    private void stopServer() throws ClientException {
        GenericResponse reponse = kvAdmin.stop();
        assertTrue(reponse.getMessage(), reponse.isSuccess());
    }

    private void enableWriteLock() throws ClientException {
        GenericResponse reponse = kvAdmin.enableWriteLock();
        assertTrue(reponse.getMessage(), reponse.isSuccess());
    }

    private void disableWriteLock() throws ClientException {
        GenericResponse reponse = kvAdmin.disableWriteLock();
        assertTrue(reponse.getMessage(), reponse.isSuccess());
    }

}
