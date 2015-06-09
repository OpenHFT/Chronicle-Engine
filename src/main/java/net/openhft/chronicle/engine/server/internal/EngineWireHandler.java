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
import net.openhft.chronicle.engine.api.*;
import net.openhft.chronicle.engine.api.collection.ValuesCollection;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.set.EntrySetView;
import net.openhft.chronicle.engine.api.set.KeySetView;
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
    @NotNull
    private final CollectionWireHandler<byte[], Set<byte[]>> keySetHandler;

    @Nullable
    private final WireHandler queueWireHandler;

    @NotNull
    private final Map<Long, String> cidToCsp;
    @NotNull
    private final MapWireHandler mapWireHandler;
    @NotNull
    private final CollectionWireHandler<Entry<byte[], byte[]>, Set<Entry<byte[], byte[]>>> entrySetHandler;
    @NotNull
    private final CollectionWireHandler<byte[], Collection<byte[]>> valuesHandler;
    @NotNull
    private final AssetTree assetTree;
    @NotNull
    private final Consumer<WireIn> metaDataConsumer;
    private boolean isSystemMessage = true;
    private final StringBuilder lastCsp = new StringBuilder();
    private final StringBuilder eventName = new StringBuilder();

    private MapHandler mh;
    private RequestContext requestContext;
    private Class viewType;
    private SessionProvider sessionProvider;

    public EngineWireHandler(@NotNull final Map<Long, String> cidToCsp,
                             @NotNull final Function<Bytes, Wire> byteToWire,
                             @NotNull final AssetTree assetTree) throws IOException {
        super(byteToWire);

        this.assetTree = assetTree;
        this.mapWireHandler = new MapWireHandler<>(cidToCsp);
        this.keySetHandler = new CollectionWireHandlerProcessor<>();
        this.queueWireHandler = null;
        this.cidToCsp = cidToCsp;
        this.entrySetHandler = new CollectionWireHandlerProcessor<>();
        this.valuesHandler = new CollectionWireHandlerProcessor<>();
        this.metaDataConsumer = wireInConsumer();
        this.sessionProvider = assetTree.getAsset("").getView(SessionProvider.class);
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



    Object view;

    @NotNull
    private Consumer<WireIn> wireInConsumer() throws IOException {
        return (wire) -> {

            // if true the next data message will be a system message
            isSystemMessage = wire.bytes().remaining() == 0;
            if (isSystemMessage) {
                if (LOG.isDebugEnabled()) LOG.debug("received system-meta-data");
                return;
            }

            try {
                readCsp(wire);
                readTid(wire);
                if (hasCspChanged(cspText)) {

                    if (LOG.isDebugEnabled())
                        LOG.debug("received meta-data:\n" + wire.bytes().toHexString());

                    requestContext = RequestContext.requestContext(cspText);
                    viewType = requestContext.viewType();
                    final Asset asset = this.assetTree.acquireAsset(viewType, requestContext);
                    view = asset.acquireView(requestContext);

                    requestContext.keyType();

                    if (viewType == MapView.class ||
                            viewType == EntrySetView.class ||
                            viewType == ValuesCollection.class ||
                            viewType == KeySetView.class) {

                        // default to string type if not provided
                        final Class kClass = requestContext.keyType() == null ? String.class
                                : requestContext.keyType();

                        final Class vClass = requestContext.valueType() == null ? String.class
                                : requestContext.valueType();

                        mh = new GenericMapHandler(kClass, vClass);
                    }
                    else
                        throw new UnsupportedOperationException("unsupported view type");


                }
            } catch (Exception e) {
                LOG.error("", e);
                rethrow(e);
            }
        };
    }

    private boolean hasCspChanged(@NotNull final StringBuilder cspText) {
        boolean result = !cspText.equals(lastCsp);

        // if it has changed remember what it changed to, for next time this method is called.
        if (result) {
            lastCsp.setLength(0);
            lastCsp.append(cspText);
        }

        return result;
    }

    private void readTid(@NotNull WireIn metaDataWire) {
        ValueIn valueIn = metaDataWire.readEventName(eventName);
        if (CoreFields.tid.contentEquals(eventName)) {
            tid = valueIn.int64();
            eventName.setLength(0);
        } else
            tid = -1;
    }

    @Override
    protected void process(@NotNull final Wire in,
                           @NotNull final Wire out,
                           @NotNull final SessionDetailsProvider sessionDetails)
            throws StreamCorruptedException {

        logYamlToStandardOut(in);

        in.readDocument(this.metaDataConsumer, (WireIn wire) -> {

            try {

                if (LOG.isDebugEnabled())
                    LOG.debug("received data:\n" + wire.bytes().toHexString());

                sessionProvider.set(sessionDetails);

                if (isSystemMessage) {
                    sessionDetails.setUserId(wire.read(() -> "username").text());
                    return;
                }

                if (mh != null) {

                    if (viewType == MapView.class) {
                        mapWireHandler.process(in, out, (MapView) view, tid, mh, requestContext);
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

                    if (viewType == ValuesCollection.class) {
                        valuesHandler.process(in, out, (ValuesCollection) view, cspText,
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
            } finally {
                sessionProvider.remove();
            }

        });
    }

    private void logYamlToStandardOut(@NotNull Wire in) {
        if (YamlLogging.showServerReads) {
            try {
                LOG.info("\nServer Reads:\n" +
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

    @Override
    public void add(WireHandler handler) {
        handlers.add(handler);
    }
}