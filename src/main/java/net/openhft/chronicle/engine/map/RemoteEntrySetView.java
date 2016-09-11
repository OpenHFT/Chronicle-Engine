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

import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.query.Query;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.query.Filter;
import net.openhft.chronicle.engine.query.RemoteQuery;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

/**
 * @author Rob Austin.
 */
public class RemoteEntrySetView<K, MV, V> extends VanillaEntrySetView<K, MV, V> {
    public RemoteEntrySetView(RequestContext context, Asset asset, @NotNull MapView<K, V> mapView) throws AssetNotFoundException {
        super(context, asset, mapView);
    }

    @Override
    @NotNull
    public Query<Map.Entry<K, V>> query() {
        return new RemoteQuery<>((subscriber, filter, contextOperations) -> {
            mapView.registerSubscriber((Subscriber) subscriber, (Filter) filter, contextOperations);
        });
    }
}
