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

package net.openhft.chronicle.engine.set;

import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.set.KeySetView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

/**
 * Created by peter.lawrey on 11/06/2015.
 */
public class VanillaKeySetView<K, V> extends AbstractCollection<K> implements KeySetView<K> {
    private final Asset asset;
    private final MapView<K, V, V> kvMapView;

    public VanillaKeySetView(RequestContext context, Asset asset, MapView<K, V, V> kvMapView) {
        this.asset = asset;
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

    @Override
    public Asset asset() {
        return asset;
    }

    @Nullable
    @Override
    public MapView<K, ?, ?> underlying() {
        return kvMapView;
    }
}
