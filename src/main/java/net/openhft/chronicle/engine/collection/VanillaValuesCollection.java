package net.openhft.chronicle.engine.collection;

import net.openhft.chronicle.engine.api.collection.ValuesCollection;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;

/**
 * Created by peter on 11/06/15.
 */
public class VanillaValuesCollection<K, V> extends AbstractCollection<V> implements ValuesCollection<V> {
    private final MapView<K, V, V> mapView;

    public VanillaValuesCollection(RequestContext requestContext, Asset asset, MapView<K, V, V> mapView) {
        this.mapView = mapView;
    }

    @Override
    public boolean contains(Object o) {
        return mapView.containsValue(o);
    }

    @Override
    public Iterator<V> iterator() {
        return mapView.underlying().valuesIterator();
    }

    @Override
    public int size() {
        return mapView.size();
    }

    @Override
    public int hashCode() {
        int h = 0;
        for (V v : this) {
            h += Objects.hashCode(v);
        }
        return h;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Collection))
            return false;
        Collection<V> collection = (Collection<V>) obj;
        if (collection.size() != size()) return false;
        for (V v : collection) {
            if (v == null || !contains(v))
                return false;
        }
        return true;
    }
}
