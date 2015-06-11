/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.engine.server.internal;

/**
 * Created by Rob Austin
 */

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.pool.StringBuilderPool;
import net.openhft.chronicle.engine.api.AssetTree;
import net.openhft.chronicle.engine.api.RequestContext;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapEventListener;
import net.openhft.chronicle.engine.collection.CollectionWireHandlerProcessor;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static net.openhft.chronicle.engine.server.internal.MapWireHandler.EventId.*;
import static net.openhft.chronicle.engine.server.internal.MapWireHandler.Params.*;
import static net.openhft.chronicle.wire.CoreFields.reply;
import static net.openhft.chronicle.wire.WriteMarshallable.EMPTY;

/**
 * @author Rob Austin.
 */
public class MapWireHandler<K, V> implements Consumer<WireHandlers> {

    private static final StringBuilderPool SBP = new StringBuilderPool();

    private BiConsumer<ValueOut, K> kToWire;
    private BiConsumer<ValueOut, V> vToWire;
    private Function<ValueIn, K> wireToK;
    private Function<ValueIn, V> wireToV;
    private RequestContext requestContext;
    private Queue<Consumer<Wire>> publisher;
    private AssetTree assetTree;

    /**
     * @param in             the data the has come in from network
     * @param out            the data that is going out to network
     * @param map            the map that is being processed
     * @param tid            the transaction id of the event
     * @param wireAdapter    adapts keys and values to and from wire
     * @param requestContext the uri of the event
     * @param publisher      used to publish events to
     * @param assetTree
     * @throws StreamCorruptedException
     */
    public void process(@NotNull final Wire in,
                        @NotNull final Wire out,
                        @NotNull Map<K, V> map,
                        long tid,
                        @NotNull final WireAdapter<K, V> wireAdapter,
                        @NotNull final RequestContext requestContext,
                        @NotNull final Queue<Consumer<Wire>> publisher,
                        @NotNull final AssetTree assetTree) throws
            StreamCorruptedException {
        this.kToWire = wireAdapter.keyToWire();
        this.vToWire = wireAdapter.valueToWire();
        this.wireToK = wireAdapter.wireToKey();
        this.wireToV = wireAdapter.wireToValue();
        this.requestContext = requestContext;
        this.publisher = publisher;
        this.assetTree = assetTree;

        try {
            this.inWire = in;
            this.outWire = out;
            this.map = map;
            charSequenceValue = map instanceof ChronicleMap && CharSequence.class == ((ChronicleMap) map).valueClass();

            this.tid = tid;
            dataConsumer.accept(in, tid);
        } catch (Exception e) {
            LOG.error("", e);
        }
    }

    public enum Params implements WireKey {
        key,
        value,
        oldValue,
        eventType, newValue
    }

    public enum EventId implements ParameterizeWireKey {
        size,
        containsKey(key),
        containsValue(value),
        get(key),
        getAndPut(key, value),
        put(key, value),
        getAndRemove(key),
        remove(key),
        clear,
        keySet,
        values,
        entrySet,
        replace(key, value),
        replaceForOld(key, oldValue, newValue),
        putIfAbsent(key, value),
        removeWithValue(key, value),
        toString,
        putAll,
        hashCode,
        createChannel,
        entrySetRestricted,
        mapForKey,
        putMapped,
        keyBuilder,
        valueBuilder,
        remoteIdentifier,
        numberOfSegments,
        subscribe;

        private final WireKey[] params;

        <P extends WireKey> EventId(P... params) {
            this.params = params;
        }

        @NotNull
        public <P extends WireKey> P[] params() {
            return (P[]) this.params;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(MapWireHandler.class);

    @NotNull
    private final Map<Long, String> cidToCsp;

    @NotNull
    private final Map<String, Long> cspToCid = new HashMap<>();

    @Nullable
    private Wire inWire = null;
    @Nullable
    private Wire outWire = null;

    private Map<K, V> map;
    private boolean charSequenceValue;

    public MapWireHandler(@NotNull final Map<Long, String> cidToCsp) throws IOException {
        this.cidToCsp = cidToCsp;
    }

    @Override
    public void accept(@NotNull final WireHandlers wireHandlers) {
    }

    private long tid;
    private final AtomicLong cid = new AtomicLong();

    /**
     * create a new cid if one does not already exist for this csp
     *
     * @param csp the csp we wish to check for a cid
     * @return the cid for this csp
     */
    private long createCid(@NotNull CharSequence csp) {
        final long newCid = cid.incrementAndGet();
        String cspStr = csp.toString();
        final Long aLong = cspToCid.putIfAbsent(cspStr, newCid);

        if (aLong != null)
            return aLong;

        cidToCsp.put(newCid, cspStr);
        return newCid;
    }

    final StringBuilder eventName = new StringBuilder();

    private final BiConsumer<WireIn, Long> dataConsumer = new BiConsumer<WireIn, Long>() {
        @Override
        public void accept(WireIn wireIn, Long inputTid) {
            try {
                eventName.setLength(0);
                final ValueIn valueIn = inWire.readEventName(eventName);

                if (put.contentEquals(eventName)) {
                    valueIn.marshallable(wire -> {
                        final Params[] params = put.params();
                        final K key = wireToK.apply(wire.read(params[0]));
                        final V value = wireToV.apply(wire.read(params[1]));
                        nullCheck(key);
                        nullCheck(value);
                        map.put(key, value);
                    });
                    return;
                }

                if (subscribe.contentEquals(eventName)) {

                    MapEventListener<K, V> listener = new MapEventListener<K, V>() {
                        private long subscriberTid = inputTid;
                        @Override
                        public void update(K key, V oldValue, V newValue) {

                            publisher.add(new Consumer<Wire>() {

                                @Override
                                public void accept(Wire publish) {
                                    publish.writeDocument(true, wire -> wire.writeEventName(CoreFields.tid).int64(subscriberTid));
                                    publish.writeDocument(false, wire -> wire.write(reply).marshallable(m -> {
                                        m.write(Params.eventType).int8(2);
                                        kToWire.accept(m.write(Params.key), key);
                                        vToWire.accept(m.write(Params.oldValue), oldValue);
                                        vToWire.accept(m.write(Params.newValue), newValue);
                                    }));
                                }
                            });

                        }

                        @Override
                        public void insert(K key, V value) {

                            publisher.add(new Consumer<Wire>() {

                                @Override
                                public void accept(Wire publish) {
                                    publish.writeDocument(true, wire -> wire.writeEventName(CoreFields.tid).int64(subscriberTid));
                                    publish.writeDocument(false, wire -> wire.write(reply).marshallable(m -> {
                                        m.write(Params.eventType).int8(1);
                                        kToWire.accept(m.write(Params.key), key);
                                        vToWire.accept(m.write(Params.newValue), value);
                                    }));
                                }
                            });
                        }

                        @Override
                        public void remove(K key, V oldValue) {
                            publisher.add(new Consumer<Wire>() {

                                @Override
                                public void accept(Wire publish) {
                                    publish.writeDocument(true, wire -> wire.writeEventName(CoreFields.tid).int64(subscriberTid));
                                    publish.writeDocument(false, wire -> wire.write(reply).marshallable(m -> {
                                        m.write(Params.eventType).int8(3);
                                        kToWire.accept(m.write(Params.key), key);
                                        vToWire.accept(m.write(Params.oldValue), oldValue);
                                    }));
                                }
                            });
                        }
                    };
                    assetTree.registerSubscriber(requestContext.name(), MapEvent.class, e -> e.apply(listener));

                    return;
                }

                if (remove.contentEquals(eventName)) {
                    final K key = wireToK.apply(valueIn);
                    nullCheck(key);
                    map.remove(key);
                    return;
                }

                outWire.writeDocument(true, wire -> outWire.writeEventName(CoreFields.tid).int64(tid));

                writeData(out -> {
                    if (clear.contentEquals(eventName)) {
                        map.clear();
                        return;
                    }

                    if (putAll.contentEquals(eventName)) {
                        valueIn.sequence(v -> {
                            while (v.hasNextSequenceItem()) {
                                valueIn.marshallable(wire -> map.put(
                                        wireToK.apply(wire.read(put.params()[0])),
                                        wireToV.apply(wire.read(put.params()[1]))));
                            }
                        });
                        return;
                    }

                    if (EventId.putIfAbsent.contentEquals(eventName)) {
                        valueIn.marshallable(wire -> {
                            final Params[] params = putIfAbsent.params();
                            final K key = wireToK.apply(wire.read(params[0]));
                            final V newValue = wireToV.apply(wire.read(params[1]));
                            final V result = map.putIfAbsent(key, newValue);

                            nullCheck(key);
                            nullCheck(newValue);

                            vToWire.accept(outWire.writeEventName(reply), result);
                        });
                        return;
                    }

                    if (size.contentEquals(eventName)) {
                        outWire.writeEventName(reply).int64(map.size());
                        return;
                    }

                    if (keySet.contentEquals(eventName) ||
                            values.contentEquals(eventName) ||
                            entrySet.contentEquals(eventName)) {
                        createProxy(eventName.toString());
                        return;
                    }

                    if (size.contentEquals(eventName)) {
                        outWire.writeEventName(reply).int64(map.size());
                        return;
                    }

                    if (containsKey.contentEquals(eventName)) {
                        final K key = wireToK.apply(valueIn);
                        nullCheck(key);
                        outWire.writeEventName(reply)
                                .bool(map.containsKey(key));
                        return;
                    }

                    if (containsValue.contentEquals(eventName)) {
                        final V value = wireToV.apply(valueIn);
                        nullCheck(value);
                        final boolean aBoolean = map.containsValue(value);
                        outWire.writeEventName(reply).bool(
                                aBoolean);
                        return;
                    }

                    if (get.contentEquals(eventName)) {
                        final K key = wireToK.apply(valueIn);
                        nullCheck(key);

                        if (charSequenceValue) {
                            StringBuilder sb = SBP.acquireStringBuilder();
                            vToWire.accept(outWire.writeEventName(reply), (V) ((ChronicleMap) map).getUsing(key, sb));

                        } else
                            vToWire.accept(outWire.writeEventName(reply), map.get(key));

                        return;
                    }

                    if (getAndPut.contentEquals(eventName)) {
                        valueIn.marshallable(wire -> {

                            final Params[] params = getAndPut.params();
                            final K key = wireToK.apply(wire.read(params[0]));
                            final V value = wireToV.apply(wire.read(params[1]));

                            nullCheck(key);
                            nullCheck(value);

                            vToWire.accept(outWire.writeEventName(reply),
                                    map.put(key, value));
                        });
                        return;
                    }

                    if (getAndRemove.contentEquals(eventName)) {
                        final K key = wireToK.apply(valueIn);
                        nullCheck(key);
                        vToWire.accept(outWire.writeEventName(reply), map.remove(key));
                        return;
                    }

                    if (replace.contentEquals(eventName)) {
                        valueIn.marshallable(wire -> {
                            final Params[] params = replace.params();
                            final K key = wireToK.apply(wire.read(params[0]));
                            final V value = wireToV.apply(wire.read(params[1]));
                            nullCheck(key);
                            nullCheck(value);
                            vToWire.accept(outWire.writeEventName(reply),
                                    map.replace(key, value));
                        });
                        return;
                    }

                    if (replaceForOld.contentEquals(eventName)) {
                        valueIn.marshallable(wire -> {
                            final Params[] params = replaceForOld.params();
                            final K key = wireToK.apply(wire.read(params[0]));
                            V oldValue = wireToV.apply(wire.read(params[1]));
                            if (charSequenceValue)
                                oldValue = (V) oldValue.toString();
                            final V newValue = wireToV.apply(wire.read(params[2]));
                            nullCheck(key);
                            nullCheck(oldValue);
                            nullCheck(newValue);
                            outWire.writeEventName(reply).bool(map.replace(key, oldValue, newValue));
                        });
                        return;
                    }

                    if (putIfAbsent.contentEquals(eventName)) {
                        valueIn.marshallable(wire -> {
                            final Params[] params = putIfAbsent.params();
                            final K key = wireToK.apply(wire.read(params[0]));
                            final V value = wireToV.apply(wire.read(params[1]));
                            nullCheck(key);
                            nullCheck(value);
                            vToWire.accept(outWire.writeEventName(reply),
                                    map.putIfAbsent(key, value));
                        });

                        return;
                    }

                    if (removeWithValue.contentEquals(eventName)) {
                        valueIn.marshallable(wire -> {
                            final Params[] params = removeWithValue.params();
                            final K key = wireToK.apply(wire.read(params[0]));
                            final V value = wireToV.apply(wire.read(params[1]));
                            nullCheck(key);
                            nullCheck(value);
                            outWire.writeEventName(reply).bool(map.remove(key, value));
                        });
                    }

                    if (hashCode.contentEquals(eventName)) {
                        outWire.writeEventName(reply).int32(map.hashCode());
                        return;
                    }

                    throw new IllegalStateException("unsupported event=" + eventName);
                });
            } catch (Exception e) {
                LOG.error("", e);
            } finally {
                if (OS.isDebug() && YamlLogging.showServerWrites) {
                    final Bytes<?> outBytes = outWire.bytes();
                    long len = outBytes.position() - CollectionWireHandlerProcessor.SIZE_OF_SIZE;
                    if (len == 0) {
                        LOG.info("--------------------------------------------\n" +
                                "server writes:\n\n<EMPTY>");

                    } else {
                        LOG.info("--------------------------------------------\n" +
                                "server writes:\n\n" +
                                Wires.fromSizePrefixedBlobs(outBytes, CollectionWireHandlerProcessor.SIZE_OF_SIZE, len));
                    }
                }
            }
        }
    };

    void nullCheck(@Nullable Object o) {
        if (o == null)
            throw new NullPointerException();
    }

    final StringBuilder cpsBuff = new StringBuilder();

    private void createProxy(final String type) {
        outWire.writeEventName(reply).type("set-proxy").writeValue()
                .marshallable(w -> {

                    cpsBuff.setLength(0);
                    cpsBuff.append("/").append(requestContext.name());
                    cpsBuff.append("?");
                    cpsBuff.append("view=").append(type);

                    final Class keyType = requestContext.keyType();
                    if (keyType != null)
                        cpsBuff.append("&keyType=").append(keyType.getName());
                    final Class valueType = requestContext.valueType();
                    if (valueType != null)
                        cpsBuff.append("&valueType=").append(valueType.getName());

                    // todo add more fields

                    w.writeEventName(CoreFields.csp).text(cpsBuff);
                    w.writeEventName(CoreFields.cid).int64(createCid(cpsBuff));
                });
    }

    /**
     * write and exceptions and rolls back if no data was written
     */
    void writeData(@NotNull Consumer<WireOut> c) {
        outWire.writeDocument(false, out -> {

            final long position = outWire.bytes().position();
            try {
                c.accept(outWire);
            } catch (Exception exception) {
                outWire.bytes().position(position);
                outWire.writeEventName(() -> "exception").throwable(exception);
            }

            // write 'reply : {} ' if no data was sent
            if (position == outWire.bytes().position()) {
                outWire.writeEventName(reply).marshallable(EMPTY);
            }
        });
    }
}
