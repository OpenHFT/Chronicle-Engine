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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.engine.client.internal.ChronicleEngine;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.MapWireHandler;
import net.openhft.chronicle.network.WireTcpHandler;
import net.openhft.chronicle.wire.*;
import net.openhft.chronicle.wire.set.SetWireHandler;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static net.openhft.chronicle.engine.client.StringUtils.endsWith;
import static net.openhft.chronicle.wire.CoreFields.cid;
import static net.openhft.chronicle.wire.CoreFields.csp;


/**
 * Created by Rob Austin
 */
public class EngineWireHandler extends WireTcpHandler implements WireHandlers {


    private static final Logger LOG = LoggerFactory.getLogger(EngineWireHandler.class);

    public static final String TEXT_WIRE = TextWire.class.getSimpleName();
    public static final String BINARY_WIRE = BinaryWire.class.getSimpleName();
    public static final String RAW_WIRE = RawWire.class.getSimpleName();

    private final CharSequence preferredWireType = new StringBuilder(TextWire.class.getSimpleName());
    private final StringBuilder cspText = new StringBuilder();


    private final SetWireHandler setWireHandler;


    @NotNull
    private final WireHandler queueWireHandler;
    private final Map<Long, CharSequence> cidToCsp;

    @NotNull
    private final ChronicleEngine chronicleEngine;
    private final MapWireHandler<byte[], byte[]> mapWireHandler;

    public EngineWireHandler(@NotNull final MapWireHandler<byte[], byte[]> mapWireHandler,
                             @NotNull final WireHandler queueWireHandler,
                             @NotNull final Map<Long, CharSequence> cidToCsp,
                             @NotNull final ChronicleEngine chronicleEngine,
                             @NotNull final SetWireHandler setWireHandler) {
        this.mapWireHandler = mapWireHandler;
        this.setWireHandler = setWireHandler;
        this.queueWireHandler = queueWireHandler;
        this.cidToCsp = cidToCsp;
        this.chronicleEngine = chronicleEngine;
    }

    private final List<WireHandler> handlers = new ArrayList<>();

    protected void publish(Wire out) {
        if (!handlers.isEmpty()) {

            final WireHandler remove = handlers.remove(handlers.size() - 1);

            try {
                remove.process(null, out);
            } catch (StreamCorruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }


    @Override
    protected void process(Wire in, Wire out) throws StreamCorruptedException {

        try {

            final StringBuilder cspText = peekType(in);
            final String serviceName = serviceName(cspText);

            if (endsWith(cspText, "#map")) {

                final ChronicleMap<byte[], byte[]> map = chronicleEngine.getMap(
                        serviceName,
                        byte[].class,
                        byte[].class);

                mapWireHandler.process(in,
                        out,
                        map,
                        cspText,
                        valueToWire,
                        wireToKey,
                        wireToValue);
                return;
            }

            if (endsWith(cspText, "#entrySet")) {

                final ChronicleMap<byte[], byte[]> map = chronicleEngine.getMap(
                        serviceName,
                        byte[].class,
                        byte[].class);


                setWireHandler.process(in, out, map.entrySet(), cspText, entryToWire, wireToEntry);
                return;
            }

            if (endsWith(cspText, "#keySet")) {

                final ChronicleMap<byte[], byte[]> map = chronicleEngine.getMap(
                        serviceName,
                        byte[].class,
                        byte[].class);


                setWireHandler.process(in, out, map.keySet(), cspText, keyToWire, wireToKey);
                return;
            }


            if (endsWith(cspText, "#queue")) {
                queueWireHandler.process(in, out);
                return;
            }

        } catch (IOException e) {
            // todo
            LOG.error("", e);
        }

    }


    /**
     * peeks the csp or if it has a cid converts the cid into a Csp and returns that
     */
    private StringBuilder peekType(@NotNull final Wire in) {

        final Bytes<?> bytes = in.bytes();

        try {
            System.out.println("--------------------------------------------\nserver reads:\n\n" +
                    Wires.fromSizePrefixedBlobs(in.bytes()));
        } catch (Exception e) {
            System.out.println("--------------------------------------------\nserver reads:\n\n" +
                    Bytes.toDebugString(in.bytes()));
        }

        long pos = bytes.position();
        try {

            inWire.readDocument(wireIn -> {

                final StringBuilder keyName = Wires.acquireStringBuilder();

                final ValueIn read = wireIn.read(keyName);
                if (csp.contentEquals(keyName)) {
                    read.text(cspText);
                } else if (cid.contentEquals(keyName)) {
                    final long cid = read.int64();
                    final CharSequence s = cidToCsp.get(cid);
                    cspText.append(s);
                }

            }, null);
        } finally {
            bytes.position(pos);
        }

        return cspText;
    }

    private String serviceName(@NotNull final StringBuilder cspText) {

        final int slash = cspText.lastIndexOf("/");
        final int hash = cspText.lastIndexOf("#");

        return (slash != -1 && slash < (cspText.length() - 1) &&
                hash != -1 && hash < (cspText.length() - 1))
                ? cspText.substring(slash + 1, hash)
                : "";
    }

    protected Wire createWriteFor(Bytes bytes) {

        if (TEXT_WIRE.contentEquals(preferredWireType))
            return new TextWire(bytes);

        if (BINARY_WIRE.contentEquals(preferredWireType))
            return new BinaryWire(bytes);

        if (RAW_WIRE.contentEquals(preferredWireType))
            return new RawWire(bytes);

        throw new IllegalStateException("preferredWireType=" + preferredWireType + " is not supported.");

    }

    @Override
    public void add(WireHandler handler) {
        handlers.add(handler);
    }

    private final BiConsumer<ValueOut, byte[]> keyToWire =
            ValueOut::object;

    private final Function<ValueIn, byte[]> wireToKey =
            v -> v.object(byte[].class);

    private final BiConsumer<ValueOut, byte[]> valueToWire = ValueOut::object;

    private final Function<ValueIn, byte[]> wireToValue =
            v -> v.object(byte[].class);


    private final BiConsumer<Map.Entry<byte[], byte[]>, ValueOut> entryToWire =
            (e, v) -> v.marshallable(w -> {
                w.write(() -> "key").object(e.getKey());
                w.write(() -> "value").object(e.getValue());
            });

    private final Function<ValueIn, Map.Entry<byte[], byte[]>> wireToEntry = new
            Function<ValueIn, Map.Entry<byte[], byte[]>>() {
                @Override
                public Map.Entry<byte[], byte[]> apply(ValueIn valueIn) {
                    return valueIn.applyMarshallable(new Function<WireIn, Map.Entry<byte[],
                            byte[]>>() {

                        @Override
                        public Map.Entry<byte[], byte[]> apply(WireIn x) {


                            final byte[] key1 = x.read(() -> "key").object(byte[].class);
                            final byte[] value = x.read(() -> "value").object(byte[].class);

                            return new Map.Entry<byte[], byte[]>() {

                                @Override
                                public byte[] getKey() {
                                    return key1;
                                }

                                @Override
                                public byte[] getValue() {
                                    return value;
                                }

                                @Override
                                public byte[] setValue(byte[] value) {
                                    throw new UnsupportedOperationException();
                                }
                            };

                        }
                    });


                }
            };


}