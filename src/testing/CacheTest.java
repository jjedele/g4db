package testing;

import app_kvServer.persistence.FIFOCache;
import app_kvServer.persistence.LFUCache;
import app_kvServer.persistence.LRUCache;
import junit.framework.TestCase;

public class CacheTest extends TestCase {

    public void testLFUCache() {
        LFUCache<String, String> cache = new LFUCache<>(2);

        cache.put("foo", "1"); // foo usage: 1
        cache.put("bar", "2"); // bar usage: 1

        cache.get("foo"); // foo usage: 2
        cache.get("bar"); // bar usage: 2
        cache.get("bar"); // bar usage: 3
        cache.get("foo"); // foo usage: 3
        cache.get("foo"); // foo usage: 4

        cache.put("baz", "3"); // baz usage: 1

        // "bar" should be ejected in favor of "baz"

        assertFalse(cache.contains("bar"));
        assertTrue(cache.contains("foo"));
        assertTrue(cache.contains("baz"));
    }

    public void testLRUCache() {
        LRUCache<String, String> cache = new LRUCache<>(10);
        cache.put("key", "value");
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3", "value3");
        cache.put("key4", "value4");
        cache.put("key5", "value5");
        cache.put("key6", "value6");

        assertTrue(cache.contains("key"));
        assertTrue(cache.contains("key1"));
        assertTrue(cache.contains("key2"));
        assertTrue(cache.contains("key3"));
        assertTrue(cache.contains("key4"));
        cache.delete("key6");
        assertFalse(cache.contains("key6"));
        assertEquals("value5", cache.get("key5").get());
    }

    public void testFIFOCache() {
        FIFOCache<String, String> fifoCache = new FIFOCache<>(10);
        fifoCache.put("key", "value");
        fifoCache.put("key1", "value1");
        fifoCache.put("key2", "value2");
        fifoCache.put("key3", "value3");
        fifoCache.put("key4", "value4");
        fifoCache.put("key5", "value5");
        fifoCache.put("key6", "value6");

        assertTrue(fifoCache.contains("key"));
        assertTrue(fifoCache.contains("key1"));
        assertTrue(fifoCache.contains("key2"));
        assertTrue(fifoCache.contains("key3"));
        assertTrue(fifoCache.contains("key4"));
        fifoCache.delete("key6");
        assertFalse(fifoCache.contains("key6"));
        assertEquals("value5", fifoCache.get("key5").get());
    }

}
