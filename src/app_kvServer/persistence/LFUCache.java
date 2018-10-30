package app_kvServer.persistence;

import java.util.*;

/**
 * This is an implementation of a all-operation O(1) LFU cache
 * as proposed by Shah et al.
 *
 * See:
 * Shah, K., Mitra, A., & Matani, D. (2010).
 * An O(1) algorithm for implementing the LFU cache eviction scheme, (1), 1–8.
 *
 * @param <K> Type of the keys
 * @param <V> Type of the values
 */
public class LFUCache<K,V>  implements Cache<K, V> {
    private class FrequencyNode {

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

    public LFUCache(int cacheSize) {
        this.cacheSize = cacheSize;
        this.byKey = new HashMap<K, ValueNode>();
        this.frequencyHead = new FrequencyNode(0);
    }

    @Override
    public V get(K key) {
        ValueNode valueNode = byKey.get(key);

        if (valueNode == null) {
            throw new NoSuchElementException("No such element: " + key);
        }
         updateUsage(valueNode, key);

        return valueNode.data;
    }

    @Override
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

    @Override
    public void delete(K key){
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

    @Override
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
        delete(target);
        System.out.println("Ejected: " + target);
    }

}
