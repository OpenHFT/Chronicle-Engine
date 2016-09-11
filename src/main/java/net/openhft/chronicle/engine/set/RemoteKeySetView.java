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

import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.query.Query;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.query.RemoteQuery;
import org.jetbrains.annotations.NotNull;

import static java.util.EnumSet.of;
import static net.openhft.chronicle.engine.api.tree.RequestContext.Operation.BOOTSTRAP;
import static net.openhft.chronicle.engine.api.tree.RequestContext.Operation.END_SUBSCRIPTION_AFTER_BOOTSTRAP;

/**
 * @author Rob Austin.
 */
public class RemoteKeySetView<K, V> extends VanillaKeySetView<K, V> {

    private MapView<K, ?> mapView;

    public RemoteKeySetView(@NotNull RequestContext context,
                            @NotNull Asset asset,
                            @NotNull MapView mapView) {
        super(context, asset, mapView);
        this.mapView = mapView;
    }

    @Override
    @NotNull
    public Query<K> query() {
        return new RemoteQuery<K>((subscriber, filter, contextOperations) ->
                mapView.registerKeySubscriber(subscriber, filter, of(
                        BOOTSTRAP, END_SUBSCRIPTION_AFTER_BOOTSTRAP)));
    }
}
