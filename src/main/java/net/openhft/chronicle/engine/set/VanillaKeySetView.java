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

package net.openhft.chronicle.engine.set;

import net.openhft.chronicle.engine.api.map.KeyValueStore;
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
    private final MapView<K, V> kvMapView;

    public VanillaKeySetView(RequestContext context, Asset asset, MapView<K, V> kvMapView) {
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
        return ((KeyValueStore<K, V>) kvMapView.underlying()).keySetIterator();
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
        @NotNull Set<K> set = (Set<K>) obj;
        if (set.size() != size()) return false;
        for (@Nullable K k : set) {
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
        return ((KeyValueStore<K, V>) kvMapView.underlying()).remove((K) o);
    }

    @Override
    public Asset asset() {
        return asset;
    }

    @Nullable
    @Override
    public MapView<K, ?> underlying() {
        return kvMapView;
    }
}
