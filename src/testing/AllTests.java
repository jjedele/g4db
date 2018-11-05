package testing;

import app_kvServer.CacheReplacementStrategy;
import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import java.io.File;


public class AllTests {

    static {
        try {
            File dataDirectory = new File(System.getProperty("java.io.tmpdir"),
                    "testserver" + System.currentTimeMillis());
            KVServer server =
                    new KVServer(50000, dataDirectory,10,
                            CacheReplacementStrategy.FIFO);
            new Thread(server).start();
            // make sure the server is up
            Thread.sleep(200);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Test suite() {
        TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
        clientSuite.addTestSuite(ConnectionTest.class);
        clientSuite.addTestSuite(InteractionTest.class);
        clientSuite.addTestSuite(AdditionalTest.class);
        clientSuite.addTestSuite(RecordReaderTest.class);
        clientSuite.addTestSuite(ProtocolTest.class);
        clientSuite.addTestSuite(DiskStorageTest.class);
        clientSuite.addTestSuite(CacheTest.class);
        return clientSuite;
    }

}
