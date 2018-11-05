package testing;

import app_kvServer.persistence.DiskStorage;
import app_kvServer.persistence.PersistenceException;
import junit.framework.TestCase;

import java.io.File;

public class DiskStorageTest extends TestCase {

    public File storageDir;
    public DiskStorage storage;

    public void setUp() {
        storageDir = new File(System.getProperty("java.io.tmpdir"),
                "test" + System.currentTimeMillis());
        storageDir.mkdirs();
        System.out.println("Using directory: " + storageDir);
        storage = new DiskStorage(storageDir);
    }

    public void testBasicPersistence() throws PersistenceException {
        storage.put("foo", "bar");

        assertEquals("bar", storage.get("foo"));
    }

    public void testPersistenceAfterRestart() throws PersistenceException {
        storage.put("foo", "bar");

        // simulate restart
        DiskStorage storage2 = new DiskStorage(storageDir);

        assertEquals("bar", storage2.get("foo"));
    }

    public void testGettingNonexistentValue() throws PersistenceException {
        Exception e = null;

        try {
            storage.get("nonexistent");
        } catch (Exception exc) {
            e = exc;
        }

        assertTrue(e instanceof PersistenceException);
    }
}