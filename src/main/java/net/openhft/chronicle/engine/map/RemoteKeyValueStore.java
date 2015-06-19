package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.collection.ClientWiredStatelessChronicleCollection;
import net.openhft.chronicle.engine.collection.ClientWiredStatelessChronicleSet;
import net.openhft.chronicle.network.connection.AbstractStatelessClient;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

import static net.openhft.chronicle.engine.server.internal.MapWireHandler.EventId;
import static net.openhft.chronicle.engine.server.internal.MapWireHandler.EventId.*;
import static net.openhft.chronicle.wire.CoreFields.stringEvent;

public class RemoteKeyValueStore<K, V> extends AbstractStatelessClient<EventId>
        implements Cloneable, ObjectKeyValueStore<K, V, V> {

    public static final Consumer<ValueOut> VOID_PARAMETERS = out -> out.marshallable(WriteMarshallable.EMPTY);

    private final Class<K> kClass;
    private final Class<V> vClass;
    private final Map<Long, String> cidToCsp = new HashMap<>();
    @NotNull
    private final RequestContext context;
    @NotNull
    private final Asset asset;

    public RemoteKeyValueStore(@NotNull final RequestContext context,
                               @NotNull Asset asset,
                               @NotNull final TcpChannelHub hub) {
        super(hub, (long) 0, toUri(context));
        this.asset = asset;
        this.kClass = context.keyType();
        this.vClass = context.valueType();
        this.context = context;

        subscriptions = asset.acquireView(ObjectKVSSubscription.class, context);
        subscriptions.setKvStore(this);
    }

    @NotNull
    private static String toUri(@NotNull final RequestContext context) {
        return "/" + context.name()
                + "?view=" + "map&keyType=" + context.keyType().getName() + "&valueType=" + context.valueType()
                .getName();
    }

    @Override
    public void close() {

    }

    @NotNull
    public File file() {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @SuppressWarnings("NullableProblems")
    public V putIfAbsent(K key, V value) {
        checkKey(key);
        checkValue(value);
        return proxyReturnTypedObject(putIfAbsent, null, vClass, key, value);
    }

    @Override
    public boolean containsValue(final V value) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void apply(@NotNull final EngineReplication.ReplicationEntry entry) {
        throw new UnsupportedOperationException("todo");
    }

    private void checkValue(@Nullable Object value) {
        if (value == null)
            throw new NullPointerException("value must not be null");
    }

    @SuppressWarnings("NullableProblems")
    public boolean remove(@Nullable Object key, Object value) {
        if (key == null)
            return false;
        checkValue(value);

        return proxyReturnBooleanWithArgs(removeWithValue, (K) key, (V) value);
    }

    @SuppressWarnings("NullableProblems")
    public boolean replace(K key, V oldValue, V newValue) {
        checkKey(key);
        checkValue(oldValue);
        checkValue(newValue);
        return proxyReturnBooleanWithArgs(replaceForOld, key, oldValue, newValue);
    }

    @Nullable
    @SuppressWarnings("NullableProblems")
    public V replace(K key, V value) {
        checkKey(key);
        checkValue(value);
        return proxyReturnTypedObject(replace, null, vClass, key, value);
    }

    @Override
    public void keysFor(final int segment, final SubscriptionConsumer<K> kConsumer) throws InvalidSubscriberException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void entriesFor(final int segment, final SubscriptionConsumer<MapEvent<K, V>> kvConsumer) throws InvalidSubscriberException {
        throw new UnsupportedOperationException("todo");
    }

    /**
     * calling this method should be avoided at all cost, as the entire {@code object} is
     * serialized. This equals can be used to compare map that extends ChronicleMap.  So two
     * Chronicle Maps that contain the same data are considered equal, even if the instances of the
     * chronicle maps were of different types
     *
     * @param object the object that you are comparing against
     * @return true if the contain the same data
     */
    @Override
    public boolean equals(@Nullable Object object) {
        if (this == object) return true;
        if (object == null || object.getClass().isAssignableFrom(Map.class))
            return false;

        final Map<? extends K, ? extends V> that = (Map<? extends K, ? extends V>) object;

        if (that.size() != longSize())
            return false;

        final Set<Map.Entry<K, V>> entries = entrySet();
        return that.entrySet().equals(entries);
    }

    @Override
    public int hashCode() {
        return proxyReturnInt(hashCode);
    }

    @NotNull

    public String toString() {
        final Iterator<Map.Entry<K, V>> entries = entrySet().iterator();
        if (!entries.hasNext())
            return "{}";

        StringBuilder sb = new StringBuilder();
        sb.append('{');

        while (entries.hasNext()) {
            final Map.Entry<K, V> e = entries.next();
            final K key = e.getKey();
            final V value = e.getValue();
            sb.append(key == this ? "(this Map)" : key);
            sb.append('=');
            sb.append(value == this ? "(this Map)" : value);
            if (!entries.hasNext())
                return sb.append('}').toString();
            sb.append(',').append(' ');
        }

        return sb.toString();
    }

    public boolean isEmpty() {
        return longSize() == 0;
    }

    public boolean containsKey(Object key) {
        checkKey(key);
        return proxyReturnBoolean(containsKey, out -> out.object(key));
    }

    /*public boolean containsValue(Object value) {
        checkValue(value);
        return proxyReturnBoolean(containsValue, out -> out.object(value));
    }

    public void putAll0(@NotNull Map<? extends K, ? extends V> map) {
        proxyReturnVoid(putAll, v ->
                        v.sequence(out -> map.entrySet().forEach(
                                e -> toParameters(put, e.getKey(), e.getValue()).accept(out)))
        );
    }*/

    @Nullable
    public V get(Object key) {
        checkKey(key);
        return this.proxyReturnTypedObject(get, null, vClass, key);
    }

    @Nullable
    public V getUsing(K key, V usingValue) {
        checkKey(key);
        final V v = this.proxyReturnTypedObject(get, usingValue, vClass, key);
        return v;
    }

    public long longSize() {
        return proxyReturnLong(size);
    }

    public boolean remove(Object key) {
        checkKey(key);
        sendEventAsync(remove, toParameters(remove, key));
        return false;
    }

    @Nullable
    @Override
    public V getAndRemove(final Object key) {
        checkKey(key);
        return proxyReturnTypedObject(getAndRemove, null, vClass, key);
    }

    private void checkKey(@Nullable Object key) {
        if (key == null)
            throw new NullPointerException("key can not be null");
    }

    public boolean put(K key, V value) {
        checkKey(key);
        checkValue(value);
        sendEventAsync(put, toParameters(put, key, value));
        return false;
    }

    @Nullable
    @Override
    public V getAndPut(final Object key, final Object value) {
        checkKey(key);
        checkValue(value);
        return proxyReturnTypedObject(getAndPut, null, vClass, key, value);
    }

    public void clear() {
        proxyReturnVoid(clear);
    }

    @Nullable
    @NotNull
    public Collection<V> values() {
        final StringBuilder csp = Wires.acquireStringBuilder();
        long cid = proxyReturnWireConsumer(values, read -> {

            final StringBuilder type = Wires.acquireAnotherStringBuilder(csp);

            read.type(type);
            return read.applyToMarshallable(w -> {
                stringEvent(CoreFields.csp, csp, w);
                final long cid0 = CoreFields.cid(w);
                cidToCsp.put(cid0, csp.toString());
                return cid0;
            });
        });

        final Function<ValueIn, V> conumer = valueIn -> valueIn.object(vClass);

        return new ClientWiredStatelessChronicleCollection<>(hub, ArrayList::new, conumer, "/" + context.name() + "?view=" + "values", cid
        );
    }

    @NotNull
    public Set<Map.Entry<K, V>> entrySet() {

        final StringBuilder csp = Wires.acquireStringBuilder();

        long cid = proxyReturnWireConsumer(entrySet, read -> {

            final StringBuilder type = Wires.acquireAnotherStringBuilder(csp);

            read.type(type);
            return read.applyToMarshallable(w -> {
                stringEvent(CoreFields.csp, csp, w);
                final long cid0 = CoreFields.cid(w);
                cidToCsp.put(cid0, csp.toString());
                return cid0;
            });
        });

        Function<ValueIn, Map.Entry<K, V>> conumer = valueIn -> valueIn.applyToMarshallable(r -> {

                    final K k = r.read(() -> "key").object(kClass);
                    final V v = r.read(() -> "value").object(vClass);

                    return new Map.Entry<K, V>() {
                        @Nullable
                        @Override
                        public K getKey() {
                            return k;
                        }

                        @Nullable
                        @Override
                        public V getValue() {
                            return v;
                        }

                        @NotNull
                        @Override
                        public V setValue(Object value) {
                            throw new UnsupportedOperationException();
                        }
                    };
                }

        );

        return new ClientWiredStatelessChronicleSet<>(hub, csp.toString(), cid, conumer);
    }

    @NotNull
    @Override
    public Iterator<Map.Entry<K, V>> entrySetIterator() {
        return entrySet().iterator();
    }

    @Nullable
    public Set<K> keySet() {

        final StringBuilder csp = Wires.acquireStringBuilder();

        long cid = proxyReturnWireConsumer(keySet, read -> {

            final StringBuilder type = Wires.acquireAnotherStringBuilder(csp);

            read.type(type);
            return read.applyToMarshallable(w -> {
                stringEvent(CoreFields.csp, csp, w);
                final long cid0 = CoreFields.cid(w);
                cidToCsp.put(cid0, csp.toString());
                return cid0;
            });
        });

        return new ClientWiredStatelessChronicleSet<>(hub,
                csp.toString(), cid, valueIn -> valueIn.object(kClass));
    }

    @SuppressWarnings("SameParameterValue")
    private boolean proxyReturnBoolean(@NotNull final EventId eventId,
                                       @Nullable final Consumer<ValueOut> consumer) {
        final long startTime = System.currentTimeMillis();
        return readBoolean(sendEvent(startTime, eventId, consumer), startTime);
    }

    @SuppressWarnings("SameParameterValue")
    private int proxyReturnInt(@NotNull final EventId eventId) {
        final long startTime = System.currentTimeMillis();
        return readInt(sendEvent(startTime, eventId, VOID_PARAMETERS), startTime);
    }

    @NotNull
    @Override
    public Asset asset() {
        return asset;
    }

    @NotNull
    @Override
    public KeyValueStore<K, V, V> underlying() {
        throw new UnsupportedOperationException();
    }

    // todo
    private final ObjectKVSSubscription<K, V, V> subscriptions;

    @NotNull
    @Override
    public ObjectKVSSubscription<K, V, V> subscription(boolean createIfAbsent) {
        return subscriptions;
    }

    @Override
    public Class<K> keyType() {
        return kClass;
    }

    @Override
    public Class<V> valueType() {
        return vClass;
    }

    class Entry implements Map.Entry<K, V> {

        final K key;
        final V value;

        /**
         * Creates new entry.
         */
        Entry(K k1, V v) {
            value = v;
            key = k1;
        }

        public final K getKey() {
            return key;
        }

        public final V getValue() {
            return value;
        }

        public final V setValue(V newValue) {
            V oldValue = value;
            RemoteKeyValueStore.this.put(getKey(), newValue);
            return oldValue;
        }

        public final boolean equals(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            final Map.Entry e = (Map.Entry) o;
            final Object k1 = getKey();
            final Object k2 = e.getKey();
            if (k1 == k2 || (k1 != null && k1.equals(k2))) {
                Object v1 = getValue();
                Object v2 = e.getValue();
                if (v1 == v2 || (v1 != null && v1.equals(v2)))
                    return true;
            }
            return false;
        }

        public final int hashCode() {
            return (key == null ? 0 : key.hashCode()) ^
                    (value == null ? 0 : value.hashCode());
        }

        @NotNull
        public final String toString() {
            return getKey() + "=" + getValue();
        }
    }
}

