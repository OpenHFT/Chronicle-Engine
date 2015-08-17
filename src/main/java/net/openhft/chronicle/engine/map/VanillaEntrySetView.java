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

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.set.EntrySetView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import org.jetbrains.annotations.NotNull;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaEntrySetView<K, MV, V> extends AbstractCollection<Entry<K, V>> implements EntrySetView<K, MV, V> {
    protected final MapView<K, V> mapView;
    private final Asset asset;

    public VanillaEntrySetView(RequestContext context, Asset asset, @NotNull MapView<K, V> mapView) throws AssetNotFoundException {
        this.asset = asset;
        this.mapView = mapView;
    }

    @NotNull
    @Override
    public Iterator<Entry<K, V>> iterator() {
        return mapView.underlying().entrySetIterator();
    }

    @Override
    public int size() {
        return mapView.size();
    }

    @Override
    public Asset asset() {
        return asset;
    }

    @Override
    public MapView<K, V> underlying() {
        return mapView;
    }

    @Override
    public int hashCode() {
        int h = 0;
        for (Entry<K, V> entry : this) {
            h += entry.hashCode();
        }
        return h;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Set))
            return false;
        Set<Entry<K, V>> set = (Set<Entry<K, V>>) obj;
        if (set.size() != size()) return false;
        for (Entry<K, V> entry : set) {
            if (entry == null)
                return false;
            K key = entry.getKey();
            if (key == null)
                return false;
            V value = mapView.get(key);
            if (!BytesUtil.equals(entry.getValue(), value))
                return false;
        }
        return true;

    }
}
