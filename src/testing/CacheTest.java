package testing;

import app_kvServer.persistence.LFUCache;
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

}
