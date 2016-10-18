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

import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.column.Column;
import net.openhft.chronicle.engine.api.column.ColumnView;
import net.openhft.chronicle.engine.api.column.Row;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Reference;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.set.EntrySetView;
import net.openhft.chronicle.engine.api.set.KeySetView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.api.tree.RequestContext.Operation;
import net.openhft.chronicle.engine.query.Filter;
import net.openhft.chronicle.wire.AbstractMarshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.util.*;

import static java.util.EnumSet.of;
import static net.openhft.chronicle.engine.api.tree.RequestContext.Operation.BOOTSTRAP;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaMapView<K, V> implements MapView<K, V>, ColumnView<K> {
    protected final Class keyClass;
    protected final Class valueType;
    protected final Asset asset;
    protected final RequestContext context;
    protected final boolean putReturnsNull;
    protected final boolean removeReturnsNull;

    private final KeyValueStore<K, V> kvStore;
    private AbstractCollection<V> values;

    public VanillaMapView(@NotNull RequestContext context,
                          @NotNull Asset asset,
                          @NotNull KeyValueStore<K, V> kvStore) {
        this.context = context;
        this.keyClass = context.keyType();
        this.valueType = context.valueType();
        this.asset = asset;
        this.kvStore = kvStore;
        this.putReturnsNull = context.putReturnsNull() != Boolean.FALSE;
        this.removeReturnsNull = context.removeReturnsNull() != Boolean.FALSE;
    }

    @Override
    public Class<K> keyType() {
        return keyClass;
    }

    @Override
    public Class<V> valueType() {
        return valueType;
    }

    @Nullable
    @Override
    public V getUsing(K key, Object usingValue) {
        return kvStore.getUsing(key, usingValue);
    }

    @NotNull
    @Override
    public KeySetView<K> keySet() {
        return asset.acquireView(KeySetView.class, context);
    }

    @NotNull
    @Override
    public Collection<V> values() {
        if (values == null) {
            values = new AbstractCollection<V>() {
                @NotNull
                public Iterator<V> iterator() {
                    return new Iterator<V>() {
                        @NotNull
                        private final Iterator<Entry<K, V>> i = entrySet().iterator();

                        public boolean hasNext() {
                            return i.hasNext();
                        }

                        public V next() {
                            return i.next().getValue();
                        }

                        public void remove() {
                            i.remove();
                        }
                    };
                }

                public int size() {
                    return VanillaMapView.this.size();
                }

                public boolean isEmpty() {
                    return VanillaMapView.this.isEmpty();
                }

                public void clear() {
                    VanillaMapView.this.clear();
                }

                public boolean contains(Object v) {
                    return VanillaMapView.this.containsValue(v);
                }
            };
        }
        return values;
    }

    @Override
    public boolean isEmpty() {
        return longSize() == 0;
    }

    @Override
    public boolean containsKey(final Object key) {
        checkKey(key);
        return keyClass.isInstance(key) && kvStore.containsKey((K) key);
    }

    @Override
    public boolean containsValue(Object value) {
        checkValue(value);
        try {
            for (int i = 0; i < kvStore.segments(); i++) {
                kvStore.entriesFor(i, e -> {
                    if (BytesUtil.equals(e.getValue(), value))
                        throw new InvalidSubscriberException();
                });

            }
            return false;
        } catch (InvalidSubscriberException e) {
            return true;
        }
    }

    protected void checkKey(@Nullable final Object key) {
        if (key == null)
            throw new NullPointerException("key can not be null");
    }

    protected void checkValue(@Nullable final Object value) {
        if (value == null)
            throw new NullPointerException("value can not be null");
    }

    @Override
    public Asset asset() {
        return asset;
    }

    @Override
    public int size(ColumnView.Query<K> query) {
        return (int) entrySet().stream()
                .filter(query::filter)
                .count();
    }

    @Override
    public KeyValueStore<K, V> underlying() {
        return kvStore;
    }

    @Nullable
    @Override
    public V get(Object key) {
        checkKey(key);
        return kvStore.isKeyType(key) ? kvStore.getUsing((K) key, null) : null;
    }

    @Nullable
    @Override
    public V put(K key, V value) {
        checkKey(key);
        checkValue(value);
        if (putReturnsNull) {
            kvStore.put(key, value);
            return null;

        } else {
            return kvStore.getAndPut(key, value);
        }
    }

    @Override
    public void set(K key, V value) {
        checkKey(key);
        checkValue(value);
        kvStore.put(key, value);
    }

    @Nullable
    @Override
    public V remove(Object key) {
        checkKey(key);
        if (!kvStore.isKeyType(key)) {
            return null;
        }
        K key2 = (K) key;
        if (removeReturnsNull) {
            kvStore.remove(key2);
            return null;

        } else {
            return kvStore.getAndRemove(key2);
        }
    }

    @Override
    public void onCellChanged(String columnName,
                              K key,
                              K oldKey,
                              Object value,
                              Object oldValue) {

        if (!(AbstractMarshallable.class.isAssignableFrom(keyType())) && "key".equals(columnName)) {
            kvStore.put(key, kvStore.getAndRemove(oldKey));
            return;
        }
        if (!(AbstractMarshallable.class.isAssignableFrom(keyType())) && "value".equals(columnName)) {
            kvStore.put(key, (V) value);
            return;
        }

        throw new UnsupportedOperationException("unknown column columnName=" + columnName);

    }

    @Override
    public void putAll(@net.openhft.chronicle.core.annotation.NotNull Map<? extends K, ? extends V> m) {
        for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public long longSize() {
        return kvStore.longSize();
    }

    @Nullable
    @Override
    public V getAndPut(K key, V value) {
        return kvStore.getAndPut(key, value);
    }

    @Nullable
    @Override
    public V getAndRemove(K key) {
        return kvStore.getAndRemove(key);
    }

    @NotNull
    @Override
    public EntrySetView<K, Object, V> entrySet() {
        //noinspection unchecked
        return asset.acquireView(EntrySetView.class);
    }

    @Override
    public void addRow(K k, Object... v) {
        if (v.length == 1)
            put(k, (V) v[0]);
        else
            throw new UnsupportedOperationException("");
    }

    @Override
    public void onRefresh(@NotNull Runnable r) {
        registerSubscriber(o -> r.run());
    }

  /*  private int compare(Map.Entry<K, ?> o1, Map.Entry<K, ?> o2) {

    }*//*
*/


    private Comparator<Map.Entry<K, V>> sort(final List<MarshableOrderBy> marshableOrderBy) {

        return (o1, o2) -> {
            for (MarshableOrderBy order : marshableOrderBy) {

                if (o1 == null && o2 == null)
                    return 0;

                if (o1 == null)
                    return -1;

                if (o2 == null)
                    return 1;

                final String column = order.column;
                int result = 0;

                if (column.equals("key")) {
                    if (o1.getKey() instanceof CharSequence)
                        result = String.CASE_INSENSITIVE_ORDER.compare((String) o1.getKey(), (String) o2.getKey());
                    else if (AbstractMarshallable.class.isAssignableFrom(keyType()))
                        throw new UnsupportedOperationException();
                    else if (Comparable.class.isAssignableFrom(keyType()))
                        result = ((Comparable) o1.getKey()).compareTo(o2.getKey());


                    if (result != 0) {
                        result *= order.isAscending ? 1 : -1;
                        return result;
                    }

                    continue;
                }


                if (column.equals("value")
                        && (!(AbstractMarshallable.class.isAssignableFrom(valueType())))) {
                    if (o1.getValue() instanceof CharSequence)
                        result = String.CASE_INSENSITIVE_ORDER.compare((String) o1.getValue(), (String)
                                o2.getValue());

                    else if (o1.getValue() instanceof Comparable)
                        result = ((Comparable) o1.getValue()).compareTo(o2.getValue());

                    if (result != 0) {
                        result *= order.isAscending ? 1 : -1;
                        return result;
                    }

                    continue;

                }

                try {
                    final Field field = o1.getValue().getClass().getDeclaredField(column);
                    field.setAccessible(true);
                    final Comparable o1Value = (Comparable) field.get(o1.getValue());
                    final Comparable o2Value = (Comparable) field.get(o2.getValue());

                    if ((!(o1Value instanceof Comparable)) &&
                            (!(o2Value instanceof Comparable)))
                        return 0;

                    if (!(o1Value instanceof Comparable))
                        return order.isAscending ? -1 : 1;

                    if (!(o2Value instanceof Comparable))
                        return order.isAscending ? 1 : -1;

                    return o1Value.compareTo(o2Value) * (order.isAscending ? 1 : -1);

                } catch (Exception e) {
                    Jvm.warn().on(VanillaMapView.class, e);
                }

            }

            return 0;
        };


    }


    @Override
    public Iterator<Row> iterator(final ColumnView.Query<K> query) {


        final Iterator<Map.Entry<K, V>> core = entrySet().stream()
                .filter(query::filter)
                .sorted(sort(query.marshableOrderBy))
                .iterator();

        Iterator<Row> result = new Iterator<Row>() {

            @Override
            public boolean hasNext() {
                return core.hasNext();
            }

            @Override
            public Row next() {
                final Map.Entry e = core.next();
                final Row row = new Row(columnNames());
                if (!(AbstractMarshallable.class.isAssignableFrom(keyType())))
                    row.add("key", e.getKey());
                else
                    throw new UnsupportedOperationException("todo");

                if (!(AbstractMarshallable.class.isAssignableFrom(valueType())))
                    row.add("value", e.getValue());
                else {
                    final AbstractMarshallable value = (AbstractMarshallable) e.getValue();

                    for (final Field declaredFields : valueType().getDeclaredFields()) {
                        if (!columnNames.contains(declaredFields.getName()))
                            continue;
                        try {
                            declaredFields.setAccessible(true);
                            row.add(declaredFields.getName(), declaredFields.get(value));
                        } catch (Exception e1) {
                            Jvm.warn().on(VanillaMapView.class, e1);
                        }
                    }
                }

                return row;
            }
        };


        long x = 0;
        while (x++ < query.fromIndex && result.hasNext()) {
            result.next();
        }


        return result;
    }

    @Override
    public boolean canDeleteRow() {
        return true;
    }


    @Override
    public void clear() {
        kvStore.clear();
    }

    @Nullable
    @Override
    public V putIfAbsent(@net.openhft.chronicle.core.annotation.NotNull K key, V value) {
        checkKey(key);
        checkValue(value);
        return kvStore.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(@net.openhft.chronicle.core.annotation.NotNull Object key, Object
            value) {
        checkKey(key);
        checkValue(value);
        return kvStore.isKeyType(key) && kvStore.removeIfEqual((K) key, (V) value);
    }

    @Override
    public boolean replace(@net.openhft.chronicle.core.annotation.NotNull K key,
                           @net.openhft.chronicle.core.annotation.NotNull V oldValue,
                           @net.openhft.chronicle.core.annotation.NotNull V newValue) {
        checkKey(key);
        checkValue(oldValue);
        checkValue(newValue);
        return kvStore.replaceIfEqual(key, oldValue, newValue);
    }

    @Nullable
    @Override
    public V replace(@net.openhft.chronicle.core.annotation.NotNull K key,
                     @net.openhft.chronicle.core.annotation.NotNull V value) {
        checkKey(key);
        checkValue(value);
        return kvStore.replace(key, value);
    }

    @Override
    public void registerTopicSubscriber(@NotNull TopicSubscriber<K, V> topicSubscriber) {
        KVSSubscription<K, V> subscription = (KVSSubscription<K, V>) asset.subscription(true);
        subscription.registerTopicSubscriber(RequestContext.requestContext().bootstrap(true).type(keyClass).type2(valueType), topicSubscriber);
    }

    @Override
    public void registerKeySubscriber(@NotNull Subscriber<K> subscriber) {
        registerKeySubscriber(subscriber, Filter.empty(), of(BOOTSTRAP));
    }

    @Override
    public void registerKeySubscriber(@NotNull Subscriber<K> subscriber,
                                      @NotNull Filter filter,
                                      @NotNull Set<Operation> operations) {

        final RequestContext rc = context.clone();
        operations.forEach(e -> e.apply(rc));

        KVSSubscription<K, V> subscription = (KVSSubscription<K, V>) asset.subscription(true);
        subscription.registerKeySubscriber(rc.type(keyClass), subscriber, filter);
    }

    @Override
    public void registerSubscriber(@NotNull Subscriber<MapEvent<K, V>> subscriber) {
        registerSubscriber(subscriber, Filter.empty(), of(BOOTSTRAP));
    }

    @Override
    public void registerSubscriber(@NotNull Subscriber<MapEvent<K, V>> subscriber,
                                   @NotNull Filter<MapEvent<K, V>> filter,
                                   @NotNull Set<Operation> operations) {

        final RequestContext rc = context.clone();
        rc.bootstrap(true).elementType(MapEvent.class);
        operations.forEach(e -> e.apply(rc));

        KVSSubscription<K, V> subscription = (KVSSubscription<K, V>) asset.subscription(true);
        subscription.registerSubscriber(rc, subscriber, filter);
    }

    @NotNull
    @Override
    public Reference<V> referenceFor(K key) {
        // TODO CE-101
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public int hashCode() {
        return entrySet().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Map) {
            Map map = (Map) obj;
            // todo use longSize()
            if (size() != map.size())
                return false;
            try {
                for (int i = 0; i < kvStore.segments(); i++) {
                    kvStore.entriesFor(i, e -> {
                        if (!BytesUtil.equals(e.getValue(), map.get(e.getKey())))
                            throw new InvalidSubscriberException();
                    });

                }
                return true;
            } catch (InvalidSubscriberException e) {
                return false;
            }
        }
        return false;
    }

    @NotNull
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        try {
            for (int i = 0; i < kvStore.segments(); i++) {
                kvStore.entriesFor(i, e -> sb.append(e.getKey()).append("=").append(e.getValue())
                        .append(", "));
            }
            if (sb.length() > 3)
                sb.setLength(sb.length() - 2);
            return sb.append("}").toString();
        } catch (Exception e) {
            sb.append(e);
            return sb.toString();
        }
    }


    @Override
    public List<Column> columns() {
        List<Column> result = new ArrayList<>();

        if (!(AbstractMarshallable.class.isAssignableFrom(keyType())))
            result.add(new Column("key", false, false, false, true, "", keyType()));

        if (!(AbstractMarshallable.class.isAssignableFrom(valueType())))
            result.add(new Column("value", false, false, false, false, "", valueType()));
        else {
            //valueType.isAssignableFrom()
            for (final Field declaredFields : valueType().getDeclaredFields()) {
                result.add(new Column(declaredFields.getName(), false, false, false, false, "",
                        declaredFields.getType()));
            }
        }

        return result;

    }

    ArrayList<String> columnNames = null;

    @Override
    public ArrayList<String> columnNames() {

        if (columnNames != null)
            return columnNames;

        LinkedHashSet<String> result = new LinkedHashSet<>();

        if (!(AbstractMarshallable.class.isAssignableFrom(keyType())))
            result.add("key");

        if (!(AbstractMarshallable.class.isAssignableFrom(valueType())))
            result.add("value");
        else {
            for (final Field declaredFields : valueType().getDeclaredFields()) {
                result.add(declaredFields.getName());
            }
        }

        columnNames = new ArrayList<>(result);
        return columnNames;
    }

}
