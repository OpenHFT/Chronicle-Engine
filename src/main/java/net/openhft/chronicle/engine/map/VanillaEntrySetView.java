package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.util.ThrowingSupplier;
import net.openhft.chronicle.engine.api.Asset;
import net.openhft.chronicle.engine.api.AssetNotFoundException;
import net.openhft.chronicle.engine.api.Assetted;
import net.openhft.chronicle.engine.api.RequestContext;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.set.EntrySetView;
import org.jetbrains.annotations.NotNull;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaEntrySetView<K, MV, V> extends AbstractCollection<Map.Entry<K, V>> implements EntrySetView<K, MV, V> {
    private Asset asset;
    private MapView<K, MV, V> underlying;

    public VanillaEntrySetView(RequestContext context, Asset asset, @NotNull ThrowingSupplier<Assetted, AssetNotFoundException> underlying) throws AssetNotFoundException {
        this.asset = asset;
        this.underlying = (MapView<K, MV, V>) underlying.get();
    }

    @NotNull
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
