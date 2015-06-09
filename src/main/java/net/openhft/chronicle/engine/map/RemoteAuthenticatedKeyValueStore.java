package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.annotation.NotNull;
import net.openhft.chronicle.core.util.ThrowingSupplier;
import net.openhft.chronicle.engine.api.*;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapReplicationEvent;
import net.openhft.chronicle.engine.collection.ClientWiredStatelessChronicleCollection;
import net.openhft.chronicle.engine.collection.ClientWiredStatelessChronicleSet;
import net.openhft.chronicle.network.connection.AbstractStatelessClient;
import net.openhft.chronicle.network.connection.TcpConnectionHub;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

import static net.openhft.chronicle.engine.server.internal.MapWireHandler.EventId;
import static net.openhft.chronicle.engine.server.internal.MapWireHandler.EventId.*;
import static net.openhft.chronicle.wire.CoreFields.stringEvent;

public class RemoteAuthenticatedKeyValueStore<K, V> extends AbstractStatelessClient<EventId>
        implements Cloneable, AuthenticatedKeyValueStore<K, V, V> {

    public static final Consumer<ValueOut> VOID_PARAMETERS = out -> out.marshallable(WriteMarshallable.EMPTY);

    private final Class<K> kClass;
    private final Class<V> vClass;
    private final Map<Long, String> cidToCsp = new HashMap<>();
    @org.jetbrains.annotations.NotNull
    private final RequestContext context;

    @SuppressWarnings("unchecked")
    public RemoteAuthenticatedKeyValueStore(@org.jetbrains.annotations.NotNull RequestContext context, @org.jetbrains.annotations.NotNull Asset asset, ThrowingSupplier<Assetted, AssetNotFoundException> underlying) throws AssetNotFoundException {
        this(context, hub(context, asset));
    }

    private RemoteAuthenticatedKeyValueStore(@org.jetbrains.annotations.NotNull @NotNull final RequestContext context,
                                             @org.jetbrains.annotations.NotNull @NotNull final TcpConnectionHub hub) {
        super(hub, (long) 0, toUri(context));
        this.kClass = context.keyType();
        this.vClass = context.valueType();
        this.context = context;

    }

    private static TcpConnectionHub hub(final RequestContext context, @org.jetbrains.annotations.NotNull final Asset asset) throws AssetNotFoundException {
        return asset.acquireView(TcpConnectionHub.class, context);
    }

    @org.jetbrains.annotations.NotNull
    private static String toUri(@org.jetbrains.annotations.NotNull final @NotNull RequestContext context) {
        return "/" + context.name()
                + "?view=" + "map&keyType=" + context.keyType().getSimpleName() + "&valueType=" + context.valueType()
                .getSimpleName();
    }

    @Override
    public void close() {

    }

    @org.jetbrains.annotations.NotNull
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
    public void entriesFor(final int segment, final SubscriptionConsumer<MapReplicationEvent<K, V>> kvConsumer) throws InvalidSubscriberException {
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

        if (that.size() != size())
            return false;

        final Set<Map.Entry<K, V>> entries = entrySet();
        return that.entrySet().equals(entries);
    }

    @Override
    public int hashCode() {
        return proxyReturnInt(hashCode);
    }

    @org.jetbrains.annotations.NotNull
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
        return size() == 0;
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

    public long size() {
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

    @org.jetbrains.annotations.NotNull
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

                        @org.jetbrains.annotations.NotNull
                        @Override
                        public V setValue(Object value) {
                            throw new UnsupportedOperationException();
                        }
                    };
                }

        );

        return new ClientWiredStatelessChronicleSet<>(hub, csp.toString(), cid, conumer);
    }

    @org.jetbrains.annotations.NotNull
    @Override
    public Iterator<Map.Entry<K, V>> entrySetIterator() {
        return entrySet().iterator();
    }

    @Nullable
    @NotNull
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
    private boolean proxyReturnBoolean(@org.jetbrains.annotations.NotNull @NotNull final EventId eventId,
                                       @Nullable final Consumer<ValueOut> consumer) {
        final long startTime = System.currentTimeMillis();
        return readBoolean(sendEvent(startTime, eventId, consumer), startTime);
    }

    @SuppressWarnings("SameParameterValue")
    private int proxyReturnInt(@org.jetbrains.annotations.NotNull @NotNull final EventId eventId) {
        final long startTime = System.currentTimeMillis();
        return readInt(sendEvent(startTime, eventId, VOID_PARAMETERS), startTime);
    }

    @org.jetbrains.annotations.NotNull
    @Override
    public Asset asset() {
        throw new UnsupportedOperationException();
    }

    @org.jetbrains.annotations.NotNull
    @Override
    public KeyValueStore<K, V, V> underlying() {
        throw new UnsupportedOperationException();
    }

    // todo
    private final SubscriptionKVSCollection<K, V, V> subscriptions = new
            VanillaSubscriptionKVSCollection<>(this);

    @org.jetbrains.annotations.NotNull
    @Override
    public SubscriptionKVSCollection<K, V, V> subscription(final boolean createIfAbsent) {
        return subscriptions;
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
            RemoteAuthenticatedKeyValueStore.this.put(getKey(), newValue);
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

        @org.jetbrains.annotations.NotNull
        @NotNull
        public final String toString() {
            return getKey() + "=" + getValue();
        }
    }
}

