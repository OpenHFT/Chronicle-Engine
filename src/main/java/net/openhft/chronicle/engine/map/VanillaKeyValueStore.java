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

import net.openhft.chronicle.engine.api.EngineReplication.ReplicationEntry;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaKeyValueStore<K, V> implements AuthenticatedKeyValueStore<K, V> {
    private final ConcurrentMap<K, V> map = new ConcurrentHashMap<>();
    private final Asset asset;
    //private final RawKVSSubscription<K, Object, V> subscriptions;

    public VanillaKeyValueStore(RequestContext context, Asset asset) {

        //this(asset);
        this.asset = asset;
        //this.subscriptions = asset.acquireView(RawKVSSubscription.class, context);
        //subscriptions.setKvStore(this);
    }

    public VanillaKeyValueStore(Asset asset) {
        this(null, asset);
        //this.asset = asset;

    }

    @Override
    public boolean put(K key, V value) {
        return map.put(key, value) != null;
    }

    @Override
    public V getAndPut(K key, V value) {
        V oldValue = map.put(key, value);
//        subscriptions.notifyEvent(oldValue == null
//                ? InsertedEvent.of(asset.fullName(), key, value)
//                : UpdatedEvent.of(asset.fullName(), key, oldValue, value));
        return oldValue;
    }

    @Override
    public boolean remove(K key) {
        return map.remove(key) != null;
    }

    @Override
    public V getAndRemove(K key) {
        V oldValue = map.remove(key);
//        if (oldValue != null)
//            subscriptions.notifyEvent(RemovedEvent.of(asset.fullName(), key, oldValue));
        return oldValue;
    }

    @Override
    public V getUsing(K key, Object value) {
        return map.get(key);
    }

    @Override
    public long longSize() {
        return map.size();
    }

    @Override
    public void keysFor(int segment, @NotNull SubscriptionConsumer<K> kConsumer) throws InvalidSubscriberException {
        SubscriptionConsumer.notifyEachEvent(map.keySet(), kConsumer);
    }

    @Override
    public void entriesFor(int segment, @NotNull SubscriptionConsumer<MapEvent<K, V>> kvConsumer) throws InvalidSubscriberException {
        SubscriptionConsumer.notifyEachEvent(map.entrySet(), e -> kvConsumer.accept(InsertedEvent.of(asset.fullName(), e.getKey(), e.getValue(), false)));
    }

    @NotNull
    @Override
    public Iterator<Map.Entry<K, V>> entrySetIterator() {
        return map.entrySet().iterator();
    }

    @NotNull
    @Override
    public Iterator<K> keySetIterator() {
        return map.keySet().iterator();
    }

    @Override
    public void clear() {
        try {
            for (int i = 0, segs = segments(); i < segs; i++)
                keysFor(i, map::remove);

        } catch (InvalidSubscriberException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public boolean containsValue(final V value) {
        return map.containsValue(value);
    }

    @Override
    public Asset asset() {
        return asset;
    }

    @Nullable
    @Override
    public KeyValueStore underlying() {
        return null;
    }

    @Override
    public void close() {

    }

    @NotNull
    @Override
    public KVSSubscription<K, V> subscription(boolean createIfAbsent) {

        //return subscriptions;
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void accept(final ReplicationEntry replicationEntry) {
        throw new UnsupportedOperationException("todo");
    }
}
