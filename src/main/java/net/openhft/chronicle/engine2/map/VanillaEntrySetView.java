package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.Asset;
import net.openhft.chronicle.engine2.api.FactoryContext;
import net.openhft.chronicle.engine2.api.map.EntrySetView;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaEntrySetView<K, V> extends AbstractCollection<Map.Entry<K, V>> implements EntrySetView<K, V> {
    private Asset asset;
    private KeyValueStore<K, V> underlying;

    public VanillaEntrySetView(FactoryContext<KeyValueStore<K, V>> context) {
        this.asset = context.parent();
        this.underlying = context.item();
    }

    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
        return underlying.entrySetIterator();
    }

    @Override
    public int size() {
        return (int) Math.min(Integer.MAX_VALUE, underlying.size());
    }

    @Override
    public void asset(Asset asset) {
        this.asset = asset;
    }

    @Override
    public Asset asset() {
        return asset;
    }

    @Override
    public void underlying(KeyValueStore<K, V> underlying) {
        this.underlying = underlying;
    }

    @Override
    public KeyValueStore<K, V> underlying() {
        return underlying;
    }
}
