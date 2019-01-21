package testing;

import client.KVAdmin;
import client.KVAdminInterface;
import client.KVStore;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.utils.HostAndPort;
import junit.framework.TestCase;
import org.junit.Test;


public class InteractionTest extends TestCase {

    private KVStore kvClient;
    private KVAdminInterface kvAdmin;

    public void setUp() {
        kvClient = new KVStore("localhost", 50000);
        kvAdmin = new KVAdmin(new HostAndPort("localhost", 50000));
        try {
            kvClient.connect();
            kvAdmin.connect();
            kvAdmin.start(true);
        } catch (Exception e) {
        }
    }

    public void tearDown() {
        kvAdmin.disconnect();
        kvClient.disconnect();
    }


    @Test
    public void testPut() {
        String key = "foo" + System.currentTimeMillis();
        String value = "bar";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
    }

    @Test
    public void testPutDisconnected() {
        kvClient.disconnect();
        String key = "foo";
        String value = "bar";
        Exception ex = null;

        try {
            kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertNotNull(ex);
    }

    @Test
    public void testUpdate() {
        String key = "updateTestValue";
        String initialValue = "initial";
        String updatedValue = "updated";

        KVMessage response = null;
        Exception ex = null;

        try {
            kvClient.put(key, initialValue);
            response = kvClient.put(key, updatedValue);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE);
        // we removed the value check because we decided not to send the value back to reduce network traffic
    }

    @Test
    public void testDelete() {
        String key = "deleteTestValue";
        String value = "toDelete";

        KVMessage response = null;
        Exception ex = null;

        try {
            kvClient.put(key, value);
            response = kvClient.put(key, "null");
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
    }

    @Test
    public void testGet() {
        String key = "foo";
        String value = "bar";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, value);
            response = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getValue().equals("bar"));
    }

    @Test
    public void testGetUnsetValue() {
        String key = "an unset value";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
    }

}