/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.engine.collection;

import net.openhft.chronicle.engine.api.collection.ValuesCollection;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;

/**
 * Created by peter on 11/06/15.
 */
public class VanillaValuesCollection<K, V> extends AbstractCollection<V> implements ValuesCollection<V> {
    private final Asset asset;
    private final MapView<K, V> mapView;

    public VanillaValuesCollection(RequestContext requestContext, Asset asset, MapView<K, V> mapView) {
        this.asset = asset;
        this.mapView = mapView;
    }

    @Override
    public boolean contains(Object o) {
        return mapView.containsValue(o);
    }

    @NotNull
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

    @Override
    public Asset asset() {
        return asset;
    }

    @Nullable
    @Override
    public MapView<?, V> underlying() {
        return mapView;
    }
}
