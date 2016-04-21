/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
