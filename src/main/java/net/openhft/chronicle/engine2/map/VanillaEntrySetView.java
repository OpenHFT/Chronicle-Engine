package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.Asset;
import net.openhft.chronicle.engine2.api.FactoryContext;
import net.openhft.chronicle.engine2.api.View;
import net.openhft.chronicle.engine2.api.map.EntrySetView;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;
import net.openhft.chronicle.engine2.session.LocalSession;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaEntrySetView<K, MV, V> extends AbstractCollection<Map.Entry<K, V>> implements EntrySetView<K, MV, V> {
    private Asset asset;
    private KeyValueStore<K, MV, V> underlying;

    public VanillaEntrySetView(FactoryContext<KeyValueStore<K, MV, V>> context) {
        this(context.parent(), context.item());
    }

    public VanillaEntrySetView(Asset asset, KeyValueStore<K, MV, V> underlying) {
        this.asset = asset;
        this.underlying = underlying;
    }

    @Override
    public View forSession(LocalSession session, Asset asset) {
        return new VanillaEntrySetView<K, MV, V>(asset, View.forSession(underlying, session, asset));
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
    public void underlying(KeyValueStore<K, MV, V> underlying) {
        this.underlying = underlying;
    }

    @Override
    public KeyValueStore<K, MV, V> underlying() {
        return underlying;
    }
}
