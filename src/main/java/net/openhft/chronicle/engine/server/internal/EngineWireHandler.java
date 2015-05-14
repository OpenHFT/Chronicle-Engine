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
import net.openhft.chronicle.network.WireTcpHandler;
import net.openhft.chronicle.wire.*;
import net.openhft.chronicle.wire.collection.CollectionWireHandler;
import net.openhft.chronicle.wire.collection.CollectionWireHandlerProcessor;
import net.openhft.chronicle.wire.map.MapWireHandler;
import net.openhft.chronicle.wire.map.MapWireHandlerProcessor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.util.*;

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

    private final CollectionWireHandler<byte[], Set<byte[]>> keySetHandler;

    @Nullable
    private final WireHandler queueWireHandler;
    private final Map<Long, String> cidToCsp;

    @NotNull
    private final ChronicleEngine chronicleEngine;
    private final MapWireHandler mapWireHandler;
    private final CollectionWireHandler<Map.Entry<byte[], byte[]>, Set<Map.Entry<byte[], byte[]>>> entrySetHandler;
    private final CollectionWireHandler<byte[], Collection<byte[]>> valuesHander;

    public EngineWireHandler(@NotNull final Map<Long, String> cidToCsp,
                             @NotNull final ChronicleEngine chronicleEngine)
                              throws IOException{
            this.mapWireHandler = new MapWireHandlerProcessor<>(cidToCsp);

        this.keySetHandler = new CollectionWireHandlerProcessor<>();
            this.queueWireHandler = null; //QueueWireHandler();
            this.cidToCsp = cidToCsp;
            this.chronicleEngine = chronicleEngine;
            this.entrySetHandler = new CollectionWireHandlerProcessor<>();
            this.valuesHander = new CollectionWireHandlerProcessor<>();
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

            if (endsWith(cspText, "?view=map")) {
                MapHandler mapHandler = MapHandler.create(cspText);
                final Map map = mapHandler.getMap(chronicleEngine, serviceName);

                mapWireHandler.process(in,
                            out,
                            map,
                            cspText,
                            mapHandler.getValueToWire(),
                            mapHandler.getWireToKey(),
                            mapHandler.getWireToValue());

                return;
            }

            if (endsWith(cspText, "?view=entrySet")) {
                MapHandler mapHandler = MapHandler.create(cspText);
                final Map map = mapHandler.getMap(chronicleEngine, serviceName);
                entrySetHandler.process(in, out, map.entrySet(), cspText, mapHandler.getEntryToWire(),
                        mapHandler.getWireToEntry(), HashSet::new);
                return;
            }

            if (endsWith(cspText, "?view=keySet")) {
                MapHandler mapHandler = MapHandler.create(cspText);
                final Map map = mapHandler.getMap(chronicleEngine, serviceName);
                keySetHandler.process(in, out, map.keySet(), cspText, mapHandler.getKeyToWire(),
                        mapHandler.getWireToKey(), HashSet::new);
                return;
            }

            if (endsWith(cspText, "?view=values")) {
                MapHandler mapHandler = MapHandler.create(cspText);
                final Map map = mapHandler.getMap(chronicleEngine, serviceName);
                valuesHander.process(in, out, map.values(), cspText, mapHandler.getKeyToWire(),
                        mapHandler.getWireToKey(), ArrayList::new);
                return;
            }


            if (endsWith(cspText, "?view=queue") && queueWireHandler != null) {
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

        if (net.openhft.chronicle.wire.YamlLogging.showServerReads) {
            try {
                System.out.println("\n\n" +
                        Wires.fromSizePrefixedBlobs(in.bytes()));
            } catch (Exception e) {
                System.out.println("\n\n" +
                        Bytes.toDebugString(in.bytes()));
            }
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
        final int hash = cspText.lastIndexOf("?view=");

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


}