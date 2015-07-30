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
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.collection.ValuesCollection;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.*;
import net.openhft.chronicle.engine.api.session.Heartbeat;
import net.openhft.chronicle.engine.api.session.SessionProvider;
import net.openhft.chronicle.engine.api.set.EntrySetView;
import net.openhft.chronicle.engine.api.set.KeySetView;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.api.tree.RequestContextInterner;
import net.openhft.chronicle.engine.api.tree.View;
import net.openhft.chronicle.engine.collection.CollectionWireHandler;
import net.openhft.chronicle.engine.map.ObjectKVSSubscription;
import net.openhft.chronicle.engine.tree.HostIdentifier;
import net.openhft.chronicle.engine.tree.TopologySubscription;
import net.openhft.chronicle.network.WireTcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.threads.api.EventLoop;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StreamCorruptedException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;

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
    private final CollectionWireHandler keySetHandler;

    @NotNull
    private final MapWireHandler mapWireHandler;
    @NotNull
    private final CollectionWireHandler entrySetHandler;
    @NotNull
    private final CollectionWireHandler valuesHandler;
    @NotNull
    private final ObjectKVSubscriptionHandler subscriptionHandler;

    @NotNull
    private final TopologySubscriptionHandler topologySubscriptionHandler;
    @NotNull
    private final TopicPublisherHandler topicPublisherHandler;
    @NotNull
    private final PublisherHandler publisherHandler;
    @NotNull
    private final ReferenceHandler referenceHandler;

    //  @NotNull
    //  private final TopologyHandler topologyHandler;

    @NotNull
    private final ReplicationHandler replicationHandler;

    @NotNull
    private final AssetTree assetTree;

    @Override
    public void onEndOfConnection(boolean heartbeatTimeOut) {
        for (final AbstractHandler abstractHandler : new AbstractHandler[]{mapWireHandler,
                subscriptionHandler, topologySubscriptionHandler,
                publisherHandler, replicationHandler}) {
            abstractHandler.onEndOfConnection(heartbeatTimeOut);
        }
    }

    @NotNull
    private final Consumer<WireIn> metaDataConsumer;
    private final StringBuilder lastCsp = new StringBuilder();
    private final StringBuilder eventName = new StringBuilder();
    @NotNull
    private final SystemHandler systemHandler;

    private WireAdapter wireAdapter;
    private View view;
    private boolean isSystemMessage = true;
    private RequestContext requestContext;
    @Nullable
    private Class viewType;
    @Nullable
    private final SessionProvider sessionProvider;
    private long tid;
    @Nullable
    private final HostIdentifier hostIdentifier;

    @Nullable
    private final EventLoop eventLoop;

    public EngineWireHandler(@NotNull final WireType byteToWire,
                             @NotNull final AssetTree assetTree,
                             @NotNull final Throttler throttler) {
        super(byteToWire);
        this.sessionProvider = assetTree.root().getView(SessionProvider.class);
        this.eventLoop = assetTree.root().findOrCreateView(EventLoop.class);
        assert eventLoop != null;
        try {
            this.eventLoop.start();
        } catch (RejectedExecutionException e) {
            LOG.debug("", e);
        }
        this.hostIdentifier = assetTree.root().findOrCreateView(HostIdentifier.class);
        this.assetTree = assetTree;
        this.mapWireHandler = new MapWireHandler<>();
        this.metaDataConsumer = wireInConsumer();
        this.keySetHandler = new CollectionWireHandler();
        this.entrySetHandler = new CollectionWireHandler();
        this.valuesHandler = new CollectionWireHandler();
        this.subscriptionHandler = new ObjectKVSubscriptionHandler(throttler);
        this.topologySubscriptionHandler = new TopologySubscriptionHandler(throttler);
        this.topicPublisherHandler = new TopicPublisherHandler();
        this.publisherHandler = new PublisherHandler();
        this.referenceHandler = new ReferenceHandler();
        this.replicationHandler = new ReplicationHandler();
        this.systemHandler = new SystemHandler();
    }

    private final RequestContextInterner requestContextInterner = new RequestContextInterner(128);

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

                    requestContext = requestContextInterner.intern(cspText);
                    viewType = requestContext.viewType();
                    if (viewType == null) {
                        if (LOG.isDebugEnabled()) LOG.debug("received system-meta-data");
                        isSystemMessage = true;
                        return;
                    }

                    view = this.assetTree.acquireView(requestContext);

                    if (viewType == MapView.class ||
                            viewType == EntrySetView.class ||
                            viewType == ValuesCollection.class ||
                            viewType == KeySetView.class ||
                            viewType == ObjectKVSSubscription.class ||
                            viewType == TopicPublisher.class ||
                            viewType == Publisher.class ||
                            viewType == Reference.class ||
                            viewType == TopologySubscription.class ||
                            viewType == Replication.class ||
                            viewType == Heartbeat.class) {

                        // default to string type if not provided
                        final Class type = requestContext.keyType() == null ? String.class
                                : requestContext.keyType();

                        final Class type2 = requestContext.valueType() == null ? String.class
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
    protected void process(@NotNull final WireIn in,
                           @NotNull final WireOut out,
                           @NotNull final SessionDetailsProvider sessionDetails)
            throws StreamCorruptedException {

        logYamlToStandardOut(in);

        in.readDocument(this.metaDataConsumer, (WireIn wire) -> {

            try {

                if (LOG.isDebugEnabled())
                    LOG.debug("received data:\n" + wire.bytes().toHexString());

                if (sessionProvider != null)
                    sessionProvider.set(sessionDetails);

                if (isSystemMessage) {
                    systemHandler.process(in, out, tid, sessionDetails);
                    return;
                }

                if (wireAdapter != null) {

                    if (viewType == MapView.class) {
                        mapWireHandler.process(in, out, (MapView) view, tid, wireAdapter,
                                requestContext);
                        return;
                    }

                    if (viewType == EntrySetView.class) {
                        entrySetHandler.process(in, out, (EntrySetView) view,
                                wireAdapter.entryToWire(),
                                wireAdapter.wireToEntry(), HashSet::new, tid);
                        return;
                    }

                    if (viewType == KeySetView.class) {
                        keySetHandler.process(in, out, (KeySetView) view,
                                wireAdapter.keyToWire(),
                                wireAdapter.wireToKey(), HashSet::new, tid);
                        return;
                    }

                    if (viewType == ValuesCollection.class) {
                        valuesHandler.process(in, out, (ValuesCollection) view,
                                wireAdapter.keyToWire(),
                                wireAdapter.wireToKey(), ArrayList::new, tid);
                        return;
                    }

                    if (viewType == ObjectKVSSubscription.class) {
                        subscriptionHandler.process(in,
                                requestContext, publisher, assetTree, tid,
                                outWire, (Subscription) view);
                        return;
                    }

                    if (viewType == TopologySubscription.class) {
                        topologySubscriptionHandler.process(in,
                                requestContext, publisher, assetTree, tid,
                                outWire, (TopologySubscription) view);
                        return;
                    }

                    if (viewType == Reference.class) {
                        referenceHandler.process(in,
                                publisher, tid,
                                (Reference) view, cspText, outWire, wireAdapter);
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
                        replicationHandler.process(in,
                                publisher, tid, outWire,
                                hostIdentifier,
                                (Replication) view, eventLoop);
                        return;
                    }
                }

                if (endsWith(cspText, "?view=queue")) {
                    // TODO
                }

            } catch (Exception e) {
                LOG.error("", e);
            } finally {
                if (sessionProvider != null)
                    sessionProvider.remove();
            }
        });
    }

    private void logYamlToStandardOut(@NotNull WireIn in) {
        if (Jvm.IS_DEBUG && YamlLogging.showServerReads) {
            try {
                LOG.info("\nServer Receives:\n" +
                        Wires.fromSizePrefixedBinaryToText(in.bytes()));
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