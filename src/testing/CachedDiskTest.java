package testing;

import app_kvServer.CacheReplacementStrategy;
import app_kvServer.persistence.CachedDiskStorage;
import app_kvServer.persistence.PersistenceException;
import junit.framework.TestCase;

import java.io.File;

public class CachedDiskTest extends TestCase {

    public void testCachedDisk() throws PersistenceException {
        File dataDirectory = new File("./data");
        CachedDiskStorage cachedDiskStorage = new CachedDiskStorage(dataDirectory, 3, CacheReplacementStrategy.LFU);

        cachedDiskStorage.put("first_key", "value_of_first_key");
        cachedDiskStorage.put("another random key", "its value");
        cachedDiskStorage.put("foo", "bar");
        assertTrue(cachedDiskStorage.contains("first_key"));
        assertFalse(cachedDiskStorage.contains("bar"));
        assertTrue(cachedDiskStorage.contains("foo"));
        assertEquals(cachedDiskStorage.put("expected_key", "value"), cachedDiskStorage.contains("expected_key"));
        assertNotSame(cachedDiskStorage.put("expected_key", "value"), cachedDiskStorage.contains("actual_key"));
    }
}
