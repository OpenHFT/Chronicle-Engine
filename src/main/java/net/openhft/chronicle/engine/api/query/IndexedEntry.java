package net.openhft.chronicle.engine.api.query;

import net.openhft.chronicle.wire.AbstractMarshallable;

/**
 * @author Rob Austin.
 */
public class IndexedEntry<K, V> extends AbstractMarshallable {

    private long index;
    private K k;
    private V v;

    IndexedEntry(K k, V v, long index) {
        this.k = k;
        this.v = v;
        this.index = index;
    }

    public K key() {
        return k;
    }

    public V value() {
        return v;
    }

    public long index() {
        return index;
    }
}
