package app_kvServer.persistence;

import javax.xml.validation.Validator;
import java.util.*;

public class LRUCache<K,V> {
    public class FrequencyNode {

        public final long value;
        public final Set<K> items;
        public FrequencyNode previous;
        public FrequencyNode next;

        public FrequencyNode(long value, FrequencyNode predecessor) {
            this(value);
            previous = predecessor;
            next = predecessor.next;
            predecessor.next.previous = this;
            predecessor.next = this;
        }

        public FrequencyNode(long value) {
            this.value = value;
            this.items = new HashSet<K>();
            previous = this;
            next = this;
        }

        public void unlink() {
            previous.next = next;
            next.previous = previous;
        }
    }

    private class ValueNode {

        public V data;
        public FrequencyNode parent;

        public ValueNode(V data, FrequencyNode parent) {
            this.data = data;
            this.parent = parent;
        }
    }

    private final int cacheSize;
    private final Map<K, ValueNode> byKey;
    private final FrequencyNode frequencyHead;

    public LRUCache(int cacheSize) {
        this.cacheSize = cacheSize;
        this.byKey = new HashMap<K, ValueNode>();
        this.frequencyHead = new FrequencyNode(0);
    }

    public V get(K key) {
        ValueNode valueNode = byKey.get(key);

        if (valueNode == null) {
            throw new NoSuchElementException("No such element: " + key);
        }
         updateUsage(valueNode, key);

        return valueNode.data;
    }

    public void put(K key, V value) {
        ValueNode valueNode = byKey.get(key);

        if (valueNode != null) {
            // update
            valueNode.data = value;
            updateUsage(valueNode, key);
        } else {
            // insert
            if (byKey.size() >= cacheSize) {
                ejectOne();
            }

            FrequencyNode freq = frequencyHead.next;

            if (freq == frequencyHead || freq.value != 1) {
                freq = new FrequencyNode(1, frequencyHead);
            }

            valueNode = new ValueNode(value, freq);
            freq.items.add(key);
            byKey.put(key, valueNode);
        }
    }

    public void remove(K key){
        ValueNode valueNode = byKey.get(key);

        if (valueNode == null){
            throw new NoSuchElementException("No such element: " + key);
        }

        FrequencyNode frequencyNode = valueNode.parent;

        frequencyNode.items.remove(key);
        if ( frequencyNode.items.isEmpty() ) {
            frequencyNode.unlink();
        }

        byKey.remove(key);
    }

    public boolean contains(K key) {
        return byKey.containsKey(key);
    }

    private void updateUsage(ValueNode valueNode, K key) {
        FrequencyNode freq = valueNode.parent;
        assert freq != frequencyHead;
        FrequencyNode nextFreq = freq.next;

        if (nextFreq == frequencyHead || nextFreq.value != freq.value + 1) {
            nextFreq = new FrequencyNode(freq.value + 1, freq);
        }
        nextFreq.items.add(key);
        valueNode.parent = nextFreq;

        freq.items.remove(valueNode);
        if (freq.items.isEmpty()) {
            freq.unlink();
        }
        freq.hashCode();
    }

    private void ejectOne() {
        if (byKey.isEmpty()) {
            return;
        }

        K target = frequencyHead.next.items.iterator().next();
        remove(target);
        System.out.println("Ejected: " + target);
    }

    public static void main(String[] args) {
        LRUCache<String, String> cache = new LRUCache<String, String>(2);
        cache.put("first", "2");
        cache.put("second", "1");

        cache.put("third", "3");

        assert !cache.contains("bar");
        assert cache.contains("foo");
        assert cache.contains("baz");
    }
}
