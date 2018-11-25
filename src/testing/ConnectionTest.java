package testing;

import client.KVStore;
import client.exceptions.ConnectionException;
import junit.framework.TestCase;

import java.net.UnknownHostException;


public class ConnectionTest extends TestCase {


    public void testConnectionSuccess() {

        Exception ex = null;

        KVStore kvClient = new KVStore("localhost", 50000);
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
    }


    public void testUnknownHost() {
        Exception ex = null;
        KVStore kvClient = new KVStore("unknown", 50000);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex instanceof ConnectionException);
    }


    public void testIllegalPort() {
        Exception ex = null;

        try {
            KVStore kvClient = new KVStore("localhost", 123456789);
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex instanceof IllegalArgumentException);
    }

}

