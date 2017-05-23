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
    private final ObjectSubscription<K, V> subscriptions;

    public VanillaSubscriptionKeyValueStore(@NotNull RequestContext context,
                                            @NotNull Asset asset,
                                            @NotNull KeyValueStore<K, V> item) {
        super(context, asset, item);
        this.subscriptions = asset.acquireView(ObjectSubscription.class, context);
        subscriptions.setKvStore(this);
    }

    @NotNull
    @Override
    public ObjectSubscription<K, V> subscription(boolean createIfAbsent) {
        return subscriptions;
    }

    @Override
    public V replace(@NotNull K key, @NotNull V value) {
        @Nullable V oldValue = kvStore.replace(key, value);
        if (oldValue != null) {
            subscriptions.notifyEvent(UpdatedEvent.of(asset.fullName(), key, oldValue, value,
                    false, !value.equals(oldValue)));
        }
        return oldValue;
    }

    @Override
    public boolean put(@NotNull K key, V value) {
        if (subscriptions.needsPrevious()) {
            return getAndPut(key, value) != null;
        }
        boolean replaced = kvStore.put(key, value);
        subscriptions.notifyEvent(replaced ?
                UpdatedEvent.of(asset.fullName(), key, null, value, false, true) :
                InsertedEvent.of(asset.fullName(), key, value, false));
        return replaced;

    }

    @Override
    public boolean remove(@NotNull K key) {
        if (subscriptions.needsPrevious()) {
            return getAndRemove(key) != null;
        }
        if (kvStore.remove(key)) {
            subscriptions.notifyEvent(RemovedEvent.of(asset.fullName(), key, null, false));
            return true;
        }
        return false;
    }

    @Override
    public boolean replaceIfEqual(@NotNull K key, V oldValue, V newValue) {
        if (kvStore.replaceIfEqual(key, oldValue, newValue)) {
            subscriptions.notifyEvent(UpdatedEvent.of(asset.fullName(), key, oldValue,
                    newValue, false, true));
            return true;
        }
        return false;
    }

    @Override
    public boolean removeIfEqual(@NotNull K key, V value) {
        if (kvStore.removeIfEqual(key, value)) {
            subscriptions.notifyEvent(RemovedEvent.of(asset.fullName(), key, value, false));
            return true;
        }
        return false;
    }

    @Nullable
    @Override
    public V putIfAbsent(@NotNull K key, V value) {
        @Nullable V ret = kvStore.putIfAbsent(key, value);
        if (ret == null)
            subscriptions.notifyEvent(InsertedEvent.of(asset.fullName(), key, value, false));
        return ret;
    }

    @Nullable
    @Override
    public V getAndPut(@NotNull K key, V value) {
        @Nullable V oldValue = kvStore.getAndPut(key, value);

        subscriptions.notifyEvent(oldValue == null
                ? InsertedEvent.of(asset.fullName(), key, value, false)
                : UpdatedEvent.of(asset.fullName(), key, oldValue, value, false, !oldValue.equals(value)));
        return oldValue;
    }

    @Nullable
    @Override
    public V getAndRemove(@NotNull K key) {
        @Nullable V oldValue = kvStore.getAndRemove(key);
        if (oldValue != null)
            subscriptions.notifyEvent(RemovedEvent.of(asset.fullName(), key, oldValue, false));
        return oldValue;
    }
}
