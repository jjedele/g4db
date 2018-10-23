package testing;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;

import java.io.IOException;


public class AllTests {

    static {
        try {
            new KVServer(50000, 10, "FIFO");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Test suite() {
        TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
        clientSuite.addTestSuite(ConnectionTest.class);
        clientSuite.addTestSuite(InteractionTest.class);
        clientSuite.addTestSuite(AdditionalTest.class);
        return clientSuite;
    }

}
