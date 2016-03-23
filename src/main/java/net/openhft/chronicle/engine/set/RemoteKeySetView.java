/*
 *
 *  *     Copyright (C) ${YEAR}  higherfrequencytrading.com
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
