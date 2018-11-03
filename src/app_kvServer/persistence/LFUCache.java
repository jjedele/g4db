package app_kvServer.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    private static final Logger LOG = LogManager.getLogger(LFUCache.class);

    // represents a class of elements with the same usage counter
    private class FrequencyNode {

        public final long value;
        public final Set<K> items;
        public FrequencyNode previous;
        public FrequencyNode next;

        FrequencyNode(long value, FrequencyNode predecessor) {
            this(value);
            previous = predecessor;
            next = predecessor.next;
            predecessor.next.previous = this;
            predecessor.next = this;
        }

        FrequencyNode(long value) {
            this.value = value;
            this.items = new HashSet<>();
            previous = this;
            next = this;
        }

        void unlink() {
            previous.next = next;
            next.previous = previous;
        }
    }

    // a element within a class of a certain usage count
    private class ValueNode {

        public V data;
        public FrequencyNode parent;

        ValueNode(V data, FrequencyNode parent) {
            this.data = data;
            this.parent = parent;
        }
    }

    private final int cacheSize;
    private final Map<K, ValueNode> byKey;
    private final FrequencyNode frequencyHead;

    /**
     * Default constructor.
     * @param cacheSize Number of elements to hold
     */
    public LFUCache(int cacheSize) {
        this.cacheSize = cacheSize;
        this.byKey = new HashMap<>();
        this.frequencyHead = new FrequencyNode(0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get(K key) {
        ValueNode valueNode = byKey.get(key);

        if (valueNode == null) {
            throw new NoSuchElementException("No such element: " + key);
        }
         updateUsage(valueNode, key);

        return valueNode.data;
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
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

        LOG.debug("Ejected element: {}", target);
    }

}
