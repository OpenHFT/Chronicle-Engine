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

package net.openhft.chronicle.engine.collection;

import net.openhft.chronicle.engine.api.collection.ValuesCollection;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
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
        return ((KeyValueStore<K, V>) mapView.underlying()).valuesIterator();
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
        @NotNull Collection<V> collection = (Collection<V>) obj;
        if (collection.size() != size()) return false;
        for (@Nullable V v : collection) {
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
