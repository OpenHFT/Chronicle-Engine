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
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.engine.api.collection.ValuesCollection;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.*;
import net.openhft.chronicle.engine.api.query.IndexQueueView;
import net.openhft.chronicle.engine.api.session.Heartbeat;
import net.openhft.chronicle.engine.api.set.EntrySetView;
import net.openhft.chronicle.engine.api.set.KeySetView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.api.tree.RequestContextInterner;
import net.openhft.chronicle.engine.cfg.UserStat;
import net.openhft.chronicle.engine.collection.CollectionWireHandler;
import net.openhft.chronicle.engine.map.ObjectSubscription;
import net.openhft.chronicle.engine.tree.HostIdentifier;
import net.openhft.chronicle.engine.tree.QueueView;
import net.openhft.chronicle.engine.tree.TopologySubscription;
import net.openhft.chronicle.network.ClientClosedProvider;
import net.openhft.chronicle.network.NetworkContextManager;
import net.openhft.chronicle.network.WireTcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.api.session.SessionProvider;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static net.openhft.chronicle.core.Jvm.rethrow;
import static net.openhft.chronicle.network.connection.CoreFields.csp;

/**
 * Created by Rob Austin
 */
public class EngineWireHandler extends WireTcpHandler<EngineWireNetworkContext> implements
        ClientClosedProvider, NetworkContextManager<EngineWireNetworkContext> {

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
    private final IndexQueueViewHandler indexQueueViewHandler;
    @NotNull
    private final ReferenceHandler referenceHandler;
    @NotNull
    private final ReplicationHandler replicationHandler;
    @NotNull
    private final ReadMarshallable metaDataConsumer;
    private final StringBuilder lastCsp = new StringBuilder();
    private final StringBuilder eventName = new StringBuilder();
    @NotNull
    private final SystemHandler systemHandler;
    private final RequestContextInterner requestContextInterner = new RequestContextInterner(128);
    private final StringBuilder currentLogMessage = new StringBuilder();
    private final StringBuilder prevLogMessage = new StringBuilder();
    @NotNull
    private Asset rootAsset;
    @Nullable
    private SessionProvider sessionProvider;
    @Nullable
    private EventLoop eventLoop;
    private boolean isServerSocket;
    private Asset contextAsset;

    private WireAdapter<?, ?> wireAdapter;
    private Object view;
    private boolean isSystemMessage = true;
    private RequestContext requestContext;

    private SessionDetailsProvider sessionDetails;

    @Nullable
    private Class viewType;
    private long tid;
    private long cid;
    private byte localIdentifier;
    private HostIdentifier hostIdentifier;

    public EngineWireHandler() {
        this.mapWireHandler = new MapWireHandler<>();
        this.metaDataConsumer = metaDataConsumer();
        this.keySetHandler = new CollectionWireHandler();
        this.entrySetHandler = new CollectionWireHandler();
        this.valuesHandler = new CollectionWireHandler();
        this.subscriptionHandler = new ObjectKVSubscriptionHandler();
        this.topologySubscriptionHandler = new TopologySubscriptionHandler();
        this.topicPublisherHandler = new TopicPublisherHandler();
        this.publisherHandler = new PublisherHandler();
        this.referenceHandler = new ReferenceHandler();
        this.replicationHandler = new ReplicationHandler();
        this.systemHandler = new SystemHandler();
        this.indexQueueViewHandler = new IndexQueueViewHandler();
    }

    @Override
    protected void onInitialize() {
        EngineWireNetworkContext nc = nc();
        if (wireType() == null && nc.wireType() != null)
            wireType(nc.wireType());
        publisher(nc.wireOutPublisher());

        rootAsset = nc.rootAsset().root();
        contextAsset = nc.isAcceptor() ? rootAsset : nc.rootAsset();
        hostIdentifier = rootAsset.findOrCreateView(HostIdentifier.class);

        if (hostIdentifier != null)
            localIdentifier = hostIdentifier.hostId();

        this.sessionProvider = rootAsset.getView(SessionProvider.class);
        this.eventLoop = rootAsset.findOrCreateView(EventLoop.class);
        assert eventLoop != null;

        try {
            this.eventLoop.start();

        } catch (RejectedExecutionException e) {
            LOG.debug("", e);
        }

        this.isServerSocket = nc.isAcceptor();
        this.sessionDetails = nc.sessionDetails();
        this.rootAsset = nc.rootAsset();
    }

    @Override
    public void onEndOfConnection(boolean heartbeatTimeOut) {
        for (final AbstractHandler abstractHandler : new AbstractHandler[]{mapWireHandler,
                subscriptionHandler, topologySubscriptionHandler,
                publisherHandler, replicationHandler}) {
            try {
                abstractHandler.onEndOfConnection(heartbeatTimeOut);

            } catch (Exception e) {
                LOG.warn("Failed while for " + abstractHandler, e);
            }
        }

        // allow time for outbound onEndOfConnection message to be sent
        Jvm.pause(100);
    }

    @NotNull
    private ReadMarshallable metaDataConsumer() {
        return (wire) -> {
            assert outWire.startUse();
            try {
                long startWritePosition = outWire.bytes().writePosition();

                // if true the next data message will be a system message
                isSystemMessage = wire.bytes().readRemaining() == 0;
                if (isSystemMessage) {
                    if (LOG.isDebugEnabled()) LOG.debug("received system-meta-data");
                    return;
                }

                readCsp(wire);
                readTid(wire);

                try {
                    if (hasCspChanged(cspText)) {

                        if (LOG.isDebugEnabled())
                            LOG.debug("received meta-data:\n" + wire.bytes().toHexString());

                        requestContext = requestContextInterner.intern(cspText);
                        final String fullName = requestContext.fullName();
                        if (!"/".equals(fullName))
                            contextAsset = this.rootAsset.acquireAsset(fullName);

                        viewType = requestContext.viewType();
                        if (viewType == null) {
                            if (LOG.isDebugEnabled()) LOG.debug("received system-meta-data");
                            isSystemMessage = true;
                            return;
                        }

                        view = contextAsset.acquireView(requestContext);

                        if (viewType == MapView.class ||
                                viewType == EntrySetView.class ||
                                viewType == ValuesCollection.class ||
                                viewType == KeySetView.class ||
                                viewType == ObjectSubscription.class ||
                                viewType == TopicPublisher.class ||
                                viewType == Publisher.class ||
                                viewType == Reference.class ||
                                viewType == TopologySubscription.class ||
                                viewType == Replication.class ||
                                viewType == QueueView.class ||
                                viewType == Heartbeat.class ||
                                viewType == IndexQueueView.class) {

                            // default to string type if not provided
                            final Class<?> type = requestContext.keyType() == null ? String.class
                                    : requestContext.keyType();

                            final Class<?> type2 = requestContext.valueType() == null ? String.class
                                    : requestContext.valueType();

                            wireAdapter = new GenericWireAdapter<>(type, type2);
                        } else {
                            throw new UnsupportedOperationException("unsupported view type");
                        }
                    }
                } catch (Throwable e) {
                    LOG.error("", e);
                    outWire.bytes().writePosition(startWritePosition);
                    outWire.writeDocument(true, w -> w.writeEventName(CoreFields.tid).int64(tid));
                    outWire.writeDocument(false, out -> out.writeEventName(() -> "exception").throwable(e));
                    logYamlToStandardOut(outWire);
                    rethrow(e);
                }
            } finally {
                assert outWire.endUse();
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
        } else {
            tid = -1;
        }
    }

    @Override
    protected void onRead(@NotNull final DocumentContext inDc,
                          @NotNull final WireOut out) {

        WireIn in = inDc.wire();
        assert in.startUse();
        sessionProvider.set(nc().sessionDetails());
        try {
            onRead0(inDc, out, in);
        } finally {
            sessionProvider.remove();
            assert in.endUse();
        }
    }

    private void onRead0(@NotNull DocumentContext inDc, @NotNull WireOut out, WireIn in) {
        if (!YamlLogging.showHeartBeats()) {
            //save the previous message (the meta-data for printing later)
            //if the message turns out not to be a system message
            prevLogMessage.setLength(0);
            prevLogMessage.append(currentLogMessage);
            currentLogMessage.setLength(0);
            logToBuffer(in, currentLogMessage, in.bytes().readPosition() - 4);
        } else {
            //log every message
            logYamlToStandardOut(in);
        }

        if (inDc.isMetaData()) {
            this.metaDataConsumer.readMarshallable(in);
        } else {

            try {

                if (LOG.isDebugEnabled())
                    LOG.debug("received data:\n" + in.bytes().toHexString());

                Consumer<WireType> wireTypeConsumer = wt -> {
                    wireType(wt);
                    checkWires(in.bytes(), out.bytes(), wireType());
                };

                if (isSystemMessage) {
                    systemHandler.process(in, out, tid, sessionDetails, getMonitoringMap(),
                            isServerSocket, this::publisher, hostIdentifier, wireTypeConsumer,
                            wireType());
                    if (!systemHandler.wasHeartBeat()) {
                        if (!YamlLogging.showHeartBeats())
                            logBufferToStandardOut(prevLogMessage.append(currentLogMessage));
                    }
                    return;
                }

                if (!YamlLogging.showHeartBeats()) {
                    logBufferToStandardOut(prevLogMessage.append(currentLogMessage));
                }

                Map<String, UserStat> userMonitoringMap = getMonitoringMap();
                if (userMonitoringMap != null) {
                    UserStat userStat = userMonitoringMap.get(sessionDetails.userId());
                    if (userStat == null) {
                        throw new AssertionError("User should have been logged in");
                    }
                    //Use timeInMillis
                    userStat.setRecentInteraction(LocalTime.now());
                    userStat.setTotalInteractions(userStat.getTotalInteractions() + 1);
                    userMonitoringMap.put(sessionDetails.userId(), userStat);
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

                    if (viewType == ObjectSubscription.class) {
                        subscriptionHandler.process(in,
                                requestContext, publisher(), contextAsset, tid,
                                outWire, (SubscriptionCollection) view);
                        return;
                    }

                    if (viewType == TopologySubscription.class) {
                        topologySubscriptionHandler.process(in,
                                requestContext, publisher(), contextAsset, tid,
                                outWire, (TopologySubscription) view);
                        return;
                    }

                    if (viewType == Reference.class) {
                        referenceHandler.process(in, requestContext,
                                publisher(), tid,
                                (Reference) view, cspText, outWire, wireAdapter);
                        return;
                    }

                    if (viewType == TopicPublisher.class || viewType == QueueView.class) {
                        topicPublisherHandler.process(in, publisher(), tid, outWire,
                                (TopicPublisher) view, wireAdapter);
                        return;
                    }

                    if (viewType == Publisher.class) {
                        publisherHandler.process(in, requestContext,
                                publisher(), tid,
                                (Publisher) view, outWire, wireAdapter);
                        return;
                    }

                    if (viewType == Replication.class) {
                        replicationHandler.process(in,
                                publisher(), tid, outWire,
                                hostIdentifier,
                                (Replication) view,
                                eventLoop);
                        return;
                    }

                    if (viewType == IndexQueueView.class) {
                        indexQueueViewHandler.process(in, requestContext, contextAsset,
                                publisher(), tid,
                                outWire);
                    }
                }

            } catch (Exception e) {
                LOG.warn("", e);

            } finally {
                if (sessionProvider != null)
                    sessionProvider.remove();
            }
        }
    }

    private Map<String, UserStat> getMonitoringMap() {
        Map<String, UserStat> userMonitoringMap = null;
        Asset userAsset = rootAsset.root().getAsset("proc/users");
        if (userAsset != null && userAsset.getView(MapView.class) != null) {
            userMonitoringMap = userAsset.getView(MapView.class);
        }
        return userMonitoringMap;
    }

    private void logYamlToStandardOut(@NotNull WireIn in) {
        if (YamlLogging.showServerReads()) {
            try {
                LOG.info("\nServer Receives:\n" +
                        Wires.fromSizePrefixedBlobs(in.bytes()));
            } catch (Exception e) {
                LOG.info("\n\n" +
                        Bytes.toString(in.bytes()));
            }
        }
    }

    private void logToBuffer(@NotNull WireIn in, StringBuilder logBuffer, long start) {
        if (YamlLogging.showServerReads()) {
            logBuffer.setLength(0);
            try {
                logBuffer.append("\nServer Receives:\n")
                        .append(Wires.fromSizePrefixedBlobs(in.bytes(), start));

            } catch (Exception e) {
                logBuffer.append("\n\n").append(Bytes.toString(in.bytes(), start, in.bytes().readLimit() - start));
            }
        }
    }

    private void logBufferToStandardOut(StringBuilder logBuffer) {
        if (logBuffer.length() > 0) {
            LOG.info("\n" + logBuffer.toString());
        }
    }

    /**
     * peeks the csp or if it has a cid converts the cid into a Csp and returns that
     */
    private void readCsp(@NotNull final WireIn wireIn) {
        final StringBuilder event = Wires.acquireStringBuilder();

        cspText.setLength(0);
        final ValueIn read = wireIn.readEventName(event);
        if (csp.contentEquals(event)) {
            read.textTo(cspText);

            tryReadEvent(wireIn, (that, wire) -> {
                final StringBuilder e = Wires.acquireStringBuilder();
                final ValueIn valueIn = wireIn.readEventName(e);
                if (!CoreFields.cid.contentEquals(e))
                    return false;

                final long cid1 = valueIn.int64();
                that.cid = cid1;
                mapWireHandler.setCid(cspText.toString(), cid1);
                return true;
            });

        } else if (CoreFields.cid.contentEquals(event)) {
            final long cid = read.int64();
            final CharSequence s = mapWireHandler.getCspForCid(cid);
            cspText.append(s);
            this.cid = cid;
        }
    }

    /**
     * if not successful, in other-words when the function returns try, will return the wire back to
     * the read location
     */
    private void tryReadEvent(@NotNull final WireIn wire,
                              @NotNull final BiFunction<EngineWireHandler, WireIn, Boolean> f) {
        final long readPosition = wire.bytes().readPosition();
        boolean success = false;
        try {
            success = f.apply(this, wire);
        } finally {
            if (!success) wire.bytes().readPosition(readPosition);
        }
    }

    @Override
    public boolean hasClientClosed() {
        return systemHandler.hasClientClosed();
    }

    public void close() {
        onEndOfConnection(false);
        publisher().close();
        super.close();
    }
}