/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.set.EntrySetView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaEntrySetView<K, MV, V> extends AbstractCollection<Entry<K, V>> implements EntrySetView<K, MV, V> {
    @NotNull
    protected final MapView<K, V> mapView;
    private final Asset asset;

    public VanillaEntrySetView(RequestContext context, Asset asset, @NotNull MapView<K, V> mapView) throws AssetNotFoundException {
        this.asset = asset;
        this.mapView = mapView;
    }

    @NotNull
    @Override
    public Iterator<Entry<K, V>> iterator() {
        return ((KeyValueStore<K,V>)mapView.underlying()).entrySetIterator();
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
        for (@NotNull Entry<K, V> entry : this) {
            h += entry.hashCode();
        }
        return h;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Set))
            return false;
        @NotNull Set<Entry<K, V>> set = (Set<Entry<K, V>>) obj;
        if (set.size() != size()) return false;
        for (@Nullable Entry<K, V> entry : set) {
            if (entry == null)
                return false;
            K key = entry.getKey();
            if (key == null)
                return false;
            @Nullable V value = mapView.get(key);
            if (!BytesUtil.equals(entry.getValue(), value))
                return false;
        }
        return true;

    }
}
