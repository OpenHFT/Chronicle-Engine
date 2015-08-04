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

import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaSubscriptionKeyValueStore<K, MV, V> extends AbstractKeyValueStore<K, V>
        implements ObjectKeyValueStore<K, V>, AuthenticatedKeyValueStore<K, V> {

    @NotNull
    private final ObjectKVSSubscription<K, V> subscriptions;

    public VanillaSubscriptionKeyValueStore(@NotNull RequestContext context,
                                            @NotNull Asset asset,
                                            @NotNull KeyValueStore<K, V> item) {
        super(context, asset, item);
        this.subscriptions = asset.acquireView(ObjectKVSSubscription.class, context);
        subscriptions.setKvStore(this);
    }

    @NotNull
    @Override
    public ObjectKVSSubscription<K, V> subscription(boolean createIfAbsent) {
        return subscriptions;
    }

    @Override
    public V replace(K key, V value) {
        V oldValue = kvStore.replace(key, value);
        if (oldValue != null) {
            subscriptions.notifyEvent(UpdatedEvent.of(asset.fullName(), key, oldValue, value));
        }
        return oldValue;
    }

    @Override
    public boolean put(K key, V value) {
        if (subscriptions.needsPrevious()) {
            return getAndPut(key, value) != null;
        }
        boolean replaced = kvStore.put(key, value);
            subscriptions.notifyEvent(replaced
                    ? InsertedEvent.of(asset.fullName(), key, value)
                    : UpdatedEvent.of(asset.fullName(), key, null, value));
        return replaced;

    }

    @Override
    public boolean remove(K key) {
        if (subscriptions.needsPrevious()) {
            return getAndRemove(key) != null;
        }
        if (kvStore.remove(key)) {
            subscriptions.notifyEvent(RemovedEvent.of(asset.fullName(), key, null));
            return true;
        }
        return false;
    }

    @Override
    public boolean replaceIfEqual(K key, V oldValue, V newValue) {
        if (kvStore.replaceIfEqual(key, oldValue, newValue)) {
            subscriptions.notifyEvent(UpdatedEvent.of(asset.fullName(), key, oldValue, newValue));
            return true;
        }
        return false;
    }

    @Override
    public boolean removeIfEqual(K key, V value) {
        if (kvStore.removeIfEqual(key, value)) {
            subscriptions.notifyEvent(RemovedEvent.of(asset.fullName(), key, value));
            return true;
        }
        return false;
    }

    @Nullable
    @Override
    public V putIfAbsent(K key, V value) {
        V ret = kvStore.putIfAbsent(key, value);
        if (ret == null)
            subscriptions.notifyEvent(InsertedEvent.of(asset.fullName(), key, value));
        return ret;
    }

    @Nullable
    @Override
    public V getAndPut(K key, V value) {
        V oldValue = kvStore.getAndPut(key, value);

            subscriptions.notifyEvent(oldValue == null
                    ? InsertedEvent.of(asset.fullName(), key, value)
                    : UpdatedEvent.of(asset.fullName(), key, oldValue, value));
        return oldValue;
    }

    @Nullable
    @Override
    public V getAndRemove(K key) {
        V oldValue = kvStore.getAndRemove(key);
        if (oldValue != null)
            subscriptions.notifyEvent(RemovedEvent.of(asset.fullName(), key, oldValue));
        return oldValue;
    }
}
