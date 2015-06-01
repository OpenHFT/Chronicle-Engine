package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.Asset;
import net.openhft.chronicle.engine2.api.Assetted;
import net.openhft.chronicle.engine2.api.RequestContext;
import net.openhft.chronicle.engine2.api.map.MapView;
import net.openhft.chronicle.engine2.api.set.EntrySetView;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaEntrySetView<K, MV, V> extends AbstractCollection<Map.Entry<K, V>> implements EntrySetView<K, MV, V> {
    private Asset asset;
    private MapView<K, MV, V> underlying;

    public VanillaEntrySetView(RequestContext context, Asset asset, Supplier<Assetted> underlying) {
        this.asset = asset;
        this.underlying = (MapView<K, MV, V>) underlying.get();
    }

    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
        return underlying.underlying().entrySetIterator();
    }

    @Override
    public int size() {
        return (int) Math.min(Integer.MAX_VALUE, underlying.size());
    }

    @Override
    public Asset asset() {
        return asset;
    }

    @Override
    public MapView<K, MV, V> underlying() {
        return underlying;
    }

    @Override
    public boolean keyedView() {
        return true;
    }
}
