package testing;

import common.hash.Range;
import junit.framework.TestCase;

public class RangeTest extends TestCase {

    public void testRangeNormal() {
        Range range = new Range(-42, 42);

        assertTrue(range.contains(0));
        assertTrue(range.contains(42));
        assertFalse(range.contains(-42));
    }

    public void testRangeWithJump() {
        Range range = new Range(Integer.MAX_VALUE - 42, Integer.MIN_VALUE + 42);

        assertTrue(range.contains(Integer.MIN_VALUE));
        assertTrue(range.contains(Integer.MIN_VALUE + 42));
        assertTrue(range.contains(Integer.MAX_VALUE));
        assertFalse(range.contains(Integer.MAX_VALUE - 42));
    }

    public void testEmptyRange() {
        Range range = new Range(42, 42);

        assertFalse(range.contains(42));
    }

}
