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

package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.engine.api.collection.ValuesCollection;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.Publisher;
import net.openhft.chronicle.engine.api.pubsub.Replication;
import net.openhft.chronicle.engine.api.pubsub.TopicPublisher;
import net.openhft.chronicle.engine.api.session.SessionProvider;
import net.openhft.chronicle.engine.api.set.EntrySetView;
import net.openhft.chronicle.engine.api.set.KeySetView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.api.tree.View;
import net.openhft.chronicle.engine.collection.CollectionWireHandler;
import net.openhft.chronicle.engine.collection.CollectionWireHandlerProcessor;
import net.openhft.chronicle.engine.map.KVSSubscription;
import net.openhft.chronicle.engine.map.ObjectKVSSubscription;
import net.openhft.chronicle.engine.tree.HostIdentifier;
import net.openhft.chronicle.network.WireTcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StreamCorruptedException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.LinkedTransferQueue;
import java.util.function.Consumer;
import java.util.function.Function;

import static net.openhft.chronicle.core.Jvm.rethrow;
import static net.openhft.chronicle.core.util.StringUtils.endsWith;
import static net.openhft.chronicle.network.connection.CoreFields.cid;
import static net.openhft.chronicle.network.connection.CoreFields.csp;

/**
 * Created by Rob Austin
 */
public class EngineWireHandler extends WireTcpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(EngineWireHandler.class);

    private final StringBuilder cspText = new StringBuilder();
    @NotNull
    private final CollectionWireHandler<byte[], Set<byte[]>> keySetHandler;

    @Nullable
    private final WireHandler queueWireHandler;

  
    @NotNull
    private final MapWireHandler mapWireHandler;
    @NotNull
    private final CollectionWireHandler<Entry<byte[], byte[]>, Set<Entry<byte[], byte[]>>> entrySetHandler;
    @NotNull
    private final CollectionWireHandler<byte[], Collection<byte[]>> valuesHandler;
    private final SubscriptionHandlerProcessor subscriptionHandler;
    private final TopicPublisherHandler topicPublisherHandler;
    private final PublisherHandler publisherHandler;
    private final ReplicationHandler replicationHandler;

    @NotNull
    private final AssetTree assetTree;
    @NotNull
    private final Consumer<WireIn> metaDataConsumer;
    private final StringBuilder lastCsp = new StringBuilder();
    private final StringBuilder eventName = new StringBuilder();

    private WireAdapter wireAdapter;
    private View view;
    private boolean isSystemMessage = true;
    private RequestContext requestContext;
    private Class viewType;
    private SessionProvider sessionProvider;
    private Queue<Consumer<Wire>> publisher = new LinkedTransferQueue<>();
    private long tid;
    private HostIdentifier hostIdentifier;
    private Asset mapView;
    private Asset asset;

    public EngineWireHandler(@NotNull final Function<Bytes, Wire> byteToWire,
                             @NotNull final AssetTree assetTree) {
        super(byteToWire);

        this.sessionProvider = assetTree.root().getView(SessionProvider.class);
        this.assetTree = assetTree;
        this.mapWireHandler = new MapWireHandler<>();
        this.queueWireHandler = null;
        this.metaDataConsumer = wireInConsumer();

        this.keySetHandler = new CollectionWireHandlerProcessor<>();
        this.entrySetHandler = new CollectionWireHandlerProcessor<>();
        this.valuesHandler = new CollectionWireHandlerProcessor<>();
        this.subscriptionHandler = new SubscriptionHandlerProcessor();
        this.topicPublisherHandler = new TopicPublisherHandler();
        this.publisherHandler = new PublisherHandler();
        this.replicationHandler = new ReplicationHandler();
    }

    protected void publish(Wire out) {
        final Consumer<Wire> wireConsumer = publisher.poll();

        if (wireConsumer != null) {
            wireConsumer.accept(out);
        }

    }

    @NotNull
    private Consumer<WireIn> wireInConsumer() {
        return (wire) -> {

            // if true the next data message will be a system message
            isSystemMessage = wire.bytes().readRemaining() == 0;
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

                    asset = this.assetTree.acquireAsset(viewType, requestContext);
                    view = asset.acquireView(requestContext);



                    mapView = this.assetTree.acquireAsset(MapView.class, requestContext);

                    requestContext.keyType();

                    if (viewType == MapView.class ||
                            viewType == EntrySetView.class ||
                            viewType == ValuesCollection.class ||
                            viewType == KeySetView.class ||
                            viewType == ObjectKVSSubscription.class ||
                            viewType == ObjectKVSSubscription.class ||
                            viewType == TopicPublisher.class ||
                            viewType == Publisher.class ||
                            viewType == Replication.class) {

                        // default to string type if not provided
                        final Class type = requestContext.type() == null ? String.class
                                : requestContext.keyType();

                        final Class type2 = requestContext.type2() == null ? String.class
                                : requestContext.valueType();

                        wireAdapter = new GenericWireAdapter(type, type2);
                    } else
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
                    sessionDetails.setUserId(wire.read(() -> "userid").text());
                    return;
                }

                if (wireAdapter != null) {

                    if (viewType == MapView.class) {
                        mapWireHandler.process(in, out, (KeyValueStore) ((MapView) view).underlying(), tid, wireAdapter,
                                requestContext);
                        return;
                    }

                    if (viewType == EntrySetView.class) {
                        entrySetHandler.process(in, out, (EntrySetView) view, cspText,
                                wireAdapter.entryToWire(),
                                wireAdapter.wireToEntry(), HashSet::new, tid);
                        return;
                    }

                    if (viewType == KeySetView.class) {
                        keySetHandler.process(in, out, (KeySetView) view, cspText,
                                wireAdapter.keyToWire(),
                                wireAdapter.wireToKey(), HashSet::new, tid);
                        return;
                    }

                    if (viewType == ValuesCollection.class) {
                        valuesHandler.process(in, out, (ValuesCollection) view, cspText,
                                wireAdapter.keyToWire(),
                                wireAdapter.wireToKey(), ArrayList::new, tid);
                        return;
                    }

                    if (viewType == ObjectKVSSubscription.class) {
                        subscriptionHandler.process(in,
                                requestContext, publisher, assetTree, tid,
                                outWire, (KVSSubscription) view);
                        return;
                    }

                    if (viewType == TopicPublisher.class) {
                        topicPublisherHandler.process(in, publisher, tid, outWire,
                                (TopicPublisher) view, wireAdapter);
                        return;
                    }

                    if (viewType == Publisher.class) {
                        publisherHandler.process(in,
                                publisher, tid,
                                (Publisher) view, outWire, wireAdapter);
                        return;
                    }

                    if (viewType == Replication.class) {
                        hostIdentifier = asset.acquireView(HostIdentifier.class, RequestContext.requestContext());
                        replicationHandler.process(in,
                                publisher, tid, outWire,
                                hostIdentifier,
                                (Replication)view);
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
                        Bytes.toString(in.bytes()));
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
            final CharSequence s = mapWireHandler.getCspForCid(cid);
            cspText.append(s);
        }
    }
}