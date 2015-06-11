package net.openhft.chronicle.engine.set;

import net.openhft.chronicle.engine.api.Asset;
import net.openhft.chronicle.engine.api.RequestContext;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.set.KeySetView;
import org.jetbrains.annotations.NotNull;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

/**
 * Created by peter.lawrey on 11/06/2015.
 */
public class VanillaKeySetView<K, V> extends AbstractCollection<K> implements KeySetView<K> {
    private final MapView<K, V, V> kvMapView;

    public VanillaKeySetView(RequestContext context, Asset asset, MapView<K, V, V> kvMapView) {
        this.kvMapView = kvMapView;
    }


    @Override
    public int size() {
        return kvMapView.size();
    }

    @Override
    public boolean contains(Object o) {
        return kvMapView.containsKey(o);
    }

    @NotNull
    @Override
    public Iterator<K> iterator() {
        return kvMapView.underlying().keySetIterator();
    }

    @Override
    public int hashCode() {
        int h = 0;
        for (K k : this) {
            h += Objects.hashCode(k);
        }
        return h;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Set))
            return false;
        Set<K> set = (Set<K>) obj;
        if (set.size() != size()) return false;
        for (K k : set) {
            if (k == null || !contains(k))
                return false;
        }
        return true;
    }

    @Override
    public boolean add(K k) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        return kvMapView.underlying().remove((K) o);
    }
}
