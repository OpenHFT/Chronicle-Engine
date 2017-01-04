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

package net.openhft.chronicle.engine.server.internal;

/**
 * Created by Rob Austin
 */

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.pool.StringBuilderPool;
import net.openhft.chronicle.core.util.SerializableBiFunction;
import net.openhft.chronicle.core.util.SerializableUpdaterWithArg;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.map.remote.RemoteKeyValueStore;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;
import java.util.function.Function;

import static net.openhft.chronicle.engine.server.internal.MapWireHandler.EventId.*;
import static net.openhft.chronicle.engine.server.internal.MapWireHandler.Params.*;
import static net.openhft.chronicle.network.connection.CoreFields.reply;

/**
 * @author Rob Austin.
 */
public class MapWireHandler<K, V> extends AbstractHandler {

    private static final StringBuilderPool SBP = new StringBuilderPool();
    private static final Logger LOG = LoggerFactory.getLogger(MapWireHandler.class);
    private final StringBuilder eventName = new StringBuilder();
    private final StringBuilder cpsBuff = new StringBuilder();
    private final CspManager cspManager;


    private BiConsumer<ValueOut, V> vToWire;
    @Nullable
    private Function<ValueIn, K> wireToK;
    @Nullable
    private Function<ValueIn, V> wireToV;

    @Nullable
    private WireIn inWire = null;
    @Nullable
    private MapView<K, V> map;
    private boolean charSequenceValue;
    private long tid;
    private final BiConsumer<WireIn, Long> dataConsumer = new BiConsumer<WireIn, Long>() {

        @SuppressWarnings("ConstantConditions")
        @Override
        public void accept(WireIn wireIn, Long inputTid) {

            try {
                eventName.setLength(0);
                @NotNull final ValueIn valueIn = inWire.readEventName(eventName);

                if (put.contentEquals(eventName)) {
                    valueIn.marshallable(wire -> {
                        @NotNull final Params[] params = put.params();

                        final K key = wireToK.apply(wire.read(params[0]));
                        final V value = wireToV.apply(wire.read(params[1]));
                        nullCheck(key);
                        nullCheck(value);
                        if (LOG.isDebugEnabled())
                            Jvm.debug().on(getClass(), "putting key=" + key);

                        map.put(key, value);
                    });
                    return;
                }

                if (remove.contentEquals(eventName)) {
                    final K key = wireToK.apply(valueIn);
                    nullCheck(key);
                    map.remove(key);
                    return;
                }

                if (update2.contentEquals(eventName)) {
                    valueIn.marshallable(wire -> {
                        @NotNull final Params[] params = update2.params();
                        @NotNull final SerializableUpdaterWithArg updater = (SerializableUpdaterWithArg) wire.read(params[0]).object(Object.class);
                        @Nullable final Object arg = wire.read(params[1]).object(Object.class);
                        map.asyncUpdate(updater, arg);
                    });
                    return;
                }

                outWire.writeDocument(true, wire -> outWire.writeEventName(CoreFields.tid).int64(tid));

                writeData(inWire , out -> {

                    if (clear.contentEquals(eventName)) {
                        map.clear();
                        return;
                    }

                    if (putAll.contentEquals(eventName)) {
                        valueIn.sequence(map, (m, v) -> {
                            while (v.hasNextSequenceItem()) {
                                valueIn.marshallable(wire -> m.put(
                                        wireToK.apply(wire.read(put.params()[0])),
                                        wireToV.apply(wire.read(put.params()[1]))));
                            }
                        });
                        return;
                    }

                    if (EventId.putIfAbsent.contentEquals(eventName)) {
                        valueIn.marshallable(wire -> {
                            @NotNull final Params[] params = putIfAbsent.params();
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
                        outWire.writeEventName(reply).int64(map.longSize());
                        return;
                    }

                    if (keySet.contentEquals(eventName) ||
                            values.contentEquals(eventName) ||
                            entrySet.contentEquals(eventName)) {
                        cspManager.createProxy(eventName.toString());
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
                        } else {
                            vToWire.accept(outWire.writeEventName(reply), map.get(key));
                        }

                        return;
                    }

                    if (getAndPut.contentEquals(eventName)) {
                        valueIn.marshallable(wire -> {

                            @NotNull final Params[] params = getAndPut.params();
                            final K key = wireToK.apply(wire.read(params[0]));
                            final V value = wireToV.apply(wire.read(params[1]));

                            nullCheck(key);
                            nullCheck(value);

                            vToWire.accept(outWire.writeEventName(reply),
                                    map.getAndPut(key, value));
                        });
                        return;
                    }

                    if (getAndRemove.contentEquals(eventName)) {
                        final K key = wireToK.apply(valueIn);
                        nullCheck(key);
                        vToWire.accept(outWire.writeEventName(reply), map.getAndRemove(key));
                        return;
                    }

                    if (replace.contentEquals(eventName)) {
                        valueIn.marshallable(wire -> {
                            @NotNull final Params[] params = replace.params();
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
                            @NotNull final Params[] params = replaceForOld.params();
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
                            @NotNull final Params[] params = putIfAbsent.params();
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
                            @NotNull final Params[] params = removeWithValue.params();
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

                    if (applyTo2.contentEquals(eventName)) {
                        valueIn.marshallable(wire -> {
                            @NotNull final Params[] params = applyTo2.params();
                            @Nullable final SerializableBiFunction function = (SerializableBiFunction) wire.read(params[0]).object(Object.class);
                            @Nullable final Object arg = wire.read(params[1]).object(Object.class);
                            //call typed object
                            outWire.writeEventName(reply).object(map.applyTo(function, arg));
                        });
                        return;
                    }

                    if (update4.contentEquals(eventName)) {
                        valueIn.marshallable(wire -> {
                            @NotNull final Params[] params = update4.params();
                            @NotNull final SerializableUpdaterWithArg updater = (SerializableUpdaterWithArg) wire.read(params[0]).object(Object.class);
                            @Nullable final Object updateArg = wire.read(params[1]).object(Object.class);
                            @NotNull final SerializableBiFunction returnFunction = (SerializableBiFunction) wire.read(params[2]).object(Object.class);
                            @Nullable final Object returnArg = wire.read(params[3]).object(Object.class);
                            outWire.writeEventName(reply).object(map.syncUpdate(updater, updateArg, returnFunction, returnArg));
                        });
                        return;
                    }

                    throw new IllegalStateException("unsupported event=" + eventName);
                });

            } catch (Exception e) {
                Jvm.warn().on(getClass(), e);
            }
        }
    };

    public MapWireHandler(CspManager cspManager) {
        this.cspManager = cspManager;
    }

    /**
     * @param in             the data the has come in from network
     * @param out            the data that is going out to network
     * @param map            the map that is being processed
     * @param tid            the transaction id of the event
     * @param wireAdapter    adapts keys and values to and from wire
     * @param requestContext the uri of the event
     */
    public void process(@NotNull final WireIn in,
                        @NotNull final WireOut out,
                        @NotNull MapView map,
                        long tid,
                        @NotNull final WireAdapter wireAdapter,
                        @NotNull final RequestContext requestContext) {
        this.vToWire = wireAdapter.valueToWire();
        this.wireToK = wireAdapter.wireToKey();
        this.wireToV = wireAdapter.wireToValue();
        this.requestContext = requestContext;

        setOutWire(out);

        try {
            this.inWire = in;
            this.outWire = out;
            this.map = map;
            charSequenceValue = map instanceof ChronicleMap &&
                    CharSequence.class == ((ChronicleMap) map).valueClass();
            assert !(map instanceof RemoteKeyValueStore) : "the server should not be a " +
                    "remove " +
                    "map";
            this.tid = tid;
            dataConsumer.accept(in, tid);
        } catch (Exception e) {
            Jvm.warn().on(getClass(), "", e);
        }
    }


    public enum Params implements WireKey {
        key,
        value,
        oldValue,
        eventType,
        newValue,
        timestamp,
        identifier,
        entry,
        updateFunction,
        updateArg,
        function,
        arg,
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
        applyTo2(function, arg),
        update2(updateFunction, updateArg),
        update4(updateFunction, updateArg, function, arg),
        bootstrap;

        private final WireKey[] params;

        <P extends WireKey> EventId(P... params) {
            this.params = params;
        }

        @NotNull
        public <P extends WireKey> P[] params() {
            return (P[]) this.params;
        }
    }
}
