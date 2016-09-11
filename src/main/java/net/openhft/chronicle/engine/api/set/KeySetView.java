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

package net.openhft.chronicle.engine.api.set;

import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.query.Queryable;
import net.openhft.chronicle.engine.api.tree.Assetted;
import net.openhft.chronicle.engine.api.tree.KeyedView;

import java.util.Set;
import java.util.stream.Stream;

/**
 * Marker interface for a set which represents the keySet() of a Map.  This may have additional method in future.
 */
public interface KeySetView<K> extends Set<K>, Assetted<MapView<K, ?>>,
        Queryable<K>, KeyedView {
    default Stream<K> stream() {
        return Set.super.stream();
    }
}
