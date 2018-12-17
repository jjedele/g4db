package testing;

import app_kvEcs.SimpleFailureDetector;
import junit.framework.TestCase;

public class SimpleFailureDetectorTest extends TestCase {

    public void testFailureDetection() throws InterruptedException {
        SimpleFailureDetector failureDetector = new SimpleFailureDetector(500);

        failureDetector.heartbeat(100);
        failureDetector.heartbeat(200);

        Thread.sleep(200);
        failureDetector.heartbeat(400);
        double suspicion1 = failureDetector.getSuspicion();
        assertTrue(suspicion1 < 1);

        Thread.sleep(1000);

        double suspicion2 = failureDetector.getSuspicion();
        assertTrue(suspicion2 >= 1);
    }
}
