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

import net.openhft.chronicle.engine.api.map.SubscriptionKeyValueStore;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import org.jetbrains.annotations.NotNull;

/**
 * Created by daniel on 08/06/15.
 */
public class AbstractSubscriptionKeyValueStore<K, MV,V> extends AbstractKeyValueStore<K, MV,V>
        implements SubscriptionKeyValueStore<K, MV,V> {
    protected AbstractSubscriptionKeyValueStore(RequestContext rc, Asset asset, @NotNull SubscriptionKeyValueStore<K, MV, V> kvStore) {
        super(rc, asset, kvStore);
    }

    @Override
    public KVSSubscription subscription(boolean createIfAbsent) {
        return ((SubscriptionKeyValueStore<K, MV,V>) kvStore).subscription(createIfAbsent);
    }
}
