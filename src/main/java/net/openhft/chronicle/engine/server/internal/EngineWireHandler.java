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
import net.openhft.chronicle.engine.Chassis;
import net.openhft.chronicle.engine.api.Asset;
import net.openhft.chronicle.engine.api.RequestContext;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.set.EntrySetView;
import net.openhft.chronicle.engine.api.set.KeySetView;
import net.openhft.chronicle.engine.api.set.ValuesView;
import net.openhft.chronicle.engine.collection.CollectionWireHandler;
import net.openhft.chronicle.engine.collection.CollectionWireHandlerProcessor;
import net.openhft.chronicle.network.WireTcpHandler;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Function;

import static net.openhft.chronicle.core.Jvm.rethrow;
import static net.openhft.chronicle.core.util.StringUtils.endsWith;
import static net.openhft.chronicle.wire.CoreFields.cid;
import static net.openhft.chronicle.wire.CoreFields.csp;

/**
 * Created by Rob Austin
 */
public class EngineWireHandler extends WireTcpHandler implements WireHandlers {

    private static final Logger LOG = LoggerFactory.getLogger(EngineWireHandler.class);

    private final StringBuilder cspText = new StringBuilder();
    private final CollectionWireHandler<byte[], Set<byte[]>> keySetHandler;

    @Nullable
    private final WireHandler queueWireHandler;
    private final Map<Long, String> cidToCsp;

    private final MapWireHandler mapWireHandler;
    private final CollectionWireHandler<Entry<byte[], byte[]>, Set<Entry<byte[], byte[]>>> entrySetHandler;
    private final CollectionWireHandler<byte[], Collection<byte[]>> valuesHander;
    private MapHandler mh;

    private final Consumer<WireIn> metaDataConsumer;
    private RequestContext requestContext;

    public EngineWireHandler(@NotNull final Map<Long, String> cidToCsp,
                             @NotNull final Function<Bytes, Wire> byteToWire)
            throws IOException {

        super(byteToWire);

        this.mapWireHandler = new MapWireHandler<>(cidToCsp);
        this.keySetHandler = new CollectionWireHandlerProcessor<>();
        this.queueWireHandler = null;
        this.cidToCsp = cidToCsp;

        this.entrySetHandler = new CollectionWireHandlerProcessor<>();
        this.valuesHander = new CollectionWireHandlerProcessor<>();
        this.metaDataConsumer = getWireInConsumer();
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


    private long tid;

    final StringBuilder lastCsp = new StringBuilder();
    final StringBuilder eventName = new StringBuilder();

    Object view;

    @NotNull
    private Consumer<WireIn> getWireInConsumer() throws IOException {
        return (metaDataWire) -> {

            try {
                readCsp(metaDataWire);
                readTid(metaDataWire);
                if (hasCspChanged(cspText)) {

                    requestContext = RequestContext.requestContext(cspText);
                    final Asset asset = Chassis.acquireAsset(requestContext);

                    view = asset.acquireView(requestContext);

                    requestContext.keyType();
                    final Class viewType = requestContext.viewType();
                    if (viewType == MapView.class ||
                            viewType == EntrySetView.class ||
                            viewType == ValuesView.class ||
                            viewType == KeySetView.class)

                        mh = new GenericMapHandler(
                                requestContext.keyType(),
                                requestContext.valueType());
                    else
                        throw new UnsupportedOperationException("unspported view type");


                }
            } catch (Exception e) {
                rethrow(e);
            }
        };
    }

    private boolean hasCspChanged(final StringBuilder cspText) {
        boolean result = !cspText.equals(lastCsp);

        // if it has changed remember what it changed to, for next time this method is called.
        if (result) {
            lastCsp.setLength(0);
            lastCsp.append(cspText);
        }

        return result;
    }

    private void readTid(WireIn metaDataWire) {
        ValueIn valueIn = metaDataWire.readEventName(eventName);
        if (CoreFields.tid.contentEquals(eventName)) {
            tid = valueIn.int64();
            eventName.setLength(0);
        } else {
            tid = -1;
        }
    }

    @Override
    protected void process(@NotNull final Wire in, @NotNull final Wire out) throws
            StreamCorruptedException {

        logYamlToStandardOut(in);

        in.readDocument(this.metaDataConsumer, (WireIn dataWire) -> {

            try {

                if (mh != null) {

                    final Class viewType = requestContext.viewType();
                    if (viewType == MapView.class) {
                        mapWireHandler.process(in, out, (MapView) view, cspText, tid, mh, requestContext);
                        return;
                    }

                    if (viewType == EntrySetView.class) {
                        entrySetHandler.process(in, out, (EntrySetView) view, cspText,
                                mh.entryToWire(),
                                mh.wireToEntry(), HashSet::new, tid);
                        return;
                    }

                    if (viewType == KeySetView.class) {
                        keySetHandler.process(in, out, (KeySetView) view, cspText,
                                mh.keyToWire(),
                                mh.wireToKey(), HashSet::new, tid);
                        return;
                    }

                    if (viewType == ValuesView.class) {
                        valuesHander.process(in, out, (ValuesView) view, cspText,
                                mh.keyToWire(),
                                mh.wireToKey(), ArrayList::new, tid);
                        return;
                    }
                }

                if (endsWith(cspText, "?view=queue") && queueWireHandler != null) {
                    queueWireHandler.process(in, out);
                }

            } catch (Exception e) {
                LOG.error("", e);
            }

        });
    }

    private void logYamlToStandardOut(@NotNull Wire in) {
        if (YamlLogging.showServerReads) {
            try {
                LOG.info("\n\n" +
                        Wires.fromSizePrefixedBlobs(in.bytes()));
            } catch (Exception e) {
                LOG.info("\n\n" +
                        Bytes.toDebugString(in.bytes()));
            }
        }
    }

    /**
     * peeks the csp or if it has a cid converts the cid into a Csp and returns that
     */
    private void readCsp(@NotNull final WireIn wireIn) {
        final StringBuilder keyName = Wires.acquireStringBuilder();

        cspText.setLength(0);
        final ValueIn read = wireIn.readEventName(keyName);
        if (csp.contentEquals(keyName)) {
            read.textTo(cspText);

        } else if (cid.contentEquals(keyName)) {
            final long cid = read.int64();
            final CharSequence s = cidToCsp.get(cid);
            cspText.append(s);
        }
    }

    private String serviceName(@NotNull final StringBuilder cspText) {
        final int slash = cspText.lastIndexOf("/");
        final int hash = cspText.lastIndexOf("?view=");

        return (slash != -1 && slash < (cspText.length() - 1) &&
                hash != -1 && hash < (cspText.length() - 1))
                ? cspText.substring(slash + 1, hash)
                : "";
    }

    @Override
    public void add(WireHandler handler) {
        handlers.add(handler);
    }
}