/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.engine.api.EngineReplication.ModificationIterator;
import net.openhft.chronicle.engine.api.pubsub.Replication;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.map.CMap2EngineReplicator.VanillaReplicatedEntry;
import net.openhft.chronicle.engine.tree.HostIdentifier;
import net.openhft.chronicle.network.cluster.AbstractSubHandler;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.ThreadLocal.withInitial;
import static net.openhft.chronicle.engine.server.internal.MapReplicationHandler.EventId.replicationEvent;
import static net.openhft.chronicle.network.connection.CoreFields.lastUpdateTime;

/**
 * Created by Rob Austin
 */
public class MapReplicationHandler extends AbstractSubHandler<EngineWireNetworkContext> implements
        Demarshallable, WriteMarshallable {

    private static final Logger LOG = LoggerFactory.getLogger(MapReplicationHandler.class);
    private final ThreadLocal<VanillaReplicatedEntry> vre = withInitial(VanillaReplicatedEntry::new);
    private Replication replication;
    private long timestamp;

    private byte localIdentifier;
    private volatile boolean closed;
    @NotNull
    private Class keyType;
    @NotNull
    private Class valueType;

    @UsedViaReflection
    private MapReplicationHandler(WireIn wire) {
        timestamp = wire.read(() -> "timestamp").int64();
        keyType = wire.read(() -> "keyType").typeLiteral();
        valueType = wire.read(() -> "valueType").typeLiteral();
    }

    private MapReplicationHandler(long timestamp, @NotNull Class keyType, @NotNull Class valueType) {
        this.timestamp = timestamp;
        this.keyType = keyType;
        this.valueType = valueType;
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write("timestamp").int64(timestamp);
        wire.write("keyType").typeLiteral(keyType);
        wire.write("valueType").typeLiteral(valueType);
    }

    @Override
    public void processData(@NotNull WireIn inWire, @NotNull WireOut outWire) {

        if (!inWire.hasMore())
            return;

        final StringBuilder eventName = Wires.acquireStringBuilder();
        final ValueIn valueIn = inWire.readEventName(eventName);

        if (lastUpdateTime.contentEquals(eventName)) {
            final long time = valueIn.int64();
            final byte id = inWire.read(() -> "id").int8();
            replication.setLastModificationTime(id, time);
            return;
        }

        // receives replication events
        if (replicationEvent.contentEquals(eventName)) {
            final VanillaReplicatedEntry entry = vre.get();
            if (localIdentifier == 3 && entry.identifier() == 2)
                System.out.println("");
            entry.clear();
            valueIn.marshallable(entry);
            replication.applyReplication(entry);
        }
    }


    @Override
    public void onInitialize(@NotNull WireOut outWire) {

        if (isClosed())
            return;

        Asset rootAsset = nc().rootAsset();
        final RequestContext requestContext = RequestContext.requestContext(csp());
        final Asset asset = rootAsset.acquireAsset(requestContext.fullName());

        replication = asset.acquireView(Replication.class, RequestContext.requestContext(asset
                .fullName()).keyType(keyType).valueType(valueType));

        // reflect back the map replication handler
        final long lastUpdateTime = replication.lastModificationTime
                ((byte) remoteIdentifier());
        WriteMarshallable writeMarshallable = newMapReplicationHandler(lastUpdateTime, keyType, valueType, csp(), cid());
        publish(writeMarshallable);

        final HostIdentifier hostIdentifier = rootAsset.findOrCreateView(HostIdentifier.class);

        if (hostIdentifier != null)
            localIdentifier = hostIdentifier.hostId();

        EventLoop eventLoop = rootAsset.findOrCreateView(EventLoop.class);
        eventLoop.start();

        final ModificationIterator mi = replication.acquireModificationIterator(
                (byte) remoteIdentifier());

        if (mi != null)
            mi.dirtyEntries(timestamp);

        if (mi == null)
            return;

        // sends replication events back to the remote client
        mi.setModificationNotifier(eventLoop::unpause);

        if (!eventLoop.isAlive() && !eventLoop.isClosed())
            throw new IllegalStateException("the event loop is not yet running !");

        eventLoop.addHandler(true, new ReplicationEventHandler(mi, (byte) remoteIdentifier()));
    }

    @NotNull
    public static WriteMarshallable newMapReplicationHandler(long lastUpdateTime, Class keyType, Class valueType, String csp, long cid) {
        final MapReplicationHandler h = new MapReplicationHandler
                (lastUpdateTime, keyType, valueType);

        return w -> w.writeDocument(true, d -> d.writeEventName(CoreFields.csp).text(csp)
                .writeEventName(CoreFields.cid).int64(cid)
                .writeEventName(CoreFields.handler).typedMarshallable(h));
    }

    @Override
    public void close() {
        this.closed = true;
    }

    public enum EventId implements ParameterizeWireKey {

        replicationEvent,
        bootstrap;

        private final WireKey[] params;

        @SafeVarargs
        <P extends WireKey> EventId(P... params) {
            this.params = params;
        }

        @NotNull
        public <P extends WireKey> P[] params() {
            //noinspection unchecked
            return (P[]) this.params;
        }
    }

    private class ReplicationEventHandler implements EventHandler, Closeable {


        private final ModificationIterator mi;
        private final byte id;
        boolean hasSentLastUpdateTime;
        long lastUpdateTime;
        boolean hasLogged;
        int count;
        long startBufferFullTimeStamp;

        ReplicationEventHandler(ModificationIterator mi, byte id) {
            this.mi = mi;
            this.id = id;
            lastUpdateTime = 0;
            hasLogged = false;
            count = 0;
            startBufferFullTimeStamp = 0;
        }

        @NotNull
        @Override
        public HandlerPriority priority() {
            return HandlerPriority.REPLICATION;
        }

        @Override
        public boolean action() throws InvalidEventHandlerException {

            if (closed || nc().connectionClosed())
                throw new InvalidEventHandlerException();

            final WireOutPublisher publisher = nc().wireOutPublisher();

            assert !closed;

            if (publisher.isClosed())
                throw new InvalidEventHandlerException("publisher is closed");

            // given the sending an event to the publish hold the chronicle map lock
            // we will send only one at a time

            if (!publisher.canTakeMoreData()) {
                if (startBufferFullTimeStamp == 0)
                    startBufferFullTimeStamp = System.currentTimeMillis();
                return false;
            }

            if (!mi.hasNext()) {

                if (startBufferFullTimeStamp != 0) {
                    long timetaken = System.currentTimeMillis() - startBufferFullTimeStamp;
                    if (timetaken > 100)
                        LOG.info("blocked - outbound buffer full=" + timetaken + "ms");
                    startBufferFullTimeStamp = 0;
                }

                // because events arrive in a bitset ( aka random ) order ( not necessary in
                // time order ) we can only be assured that the latest time of
                // the last event is really the latest time, once all the events
                // have been received, we know when we have received all events
                // when there are no more events to process.
                if (!hasSentLastUpdateTime && lastUpdateTime > 0) {

                    publisher.put(null, w -> {
                        w.writeDocument(true, d -> d.write(CoreFields.cid).int64(cid()));
                        w.writeDocument(false, d -> {
                            d.writeEventName(CoreFields.lastUpdateTime).int64(lastUpdateTime);
                            d.write(() -> "id").int8(id);
                        });
                    });
                    hasSentLastUpdateTime = true;
                }
                return false;
            }

            mi.nextEntry(e -> publisher.put(null, w -> {
                assert e.remoteIdentifier() != localIdentifier;
                long newlastUpdateTime = Math.max(lastUpdateTime, e.timestamp());

                if (newlastUpdateTime > lastUpdateTime) {
                    hasSentLastUpdateTime = false;
                    lastUpdateTime = newlastUpdateTime;
                }

                w.writeDocument(true, d -> d.write(CoreFields.cid).int64(cid()));
                w.writeDocument(false,
                        d -> {
                            d.writeEventName(replicationEvent).marshallable(e);
                            d.writeComment("isAcceptor=" + nc().isAcceptor());
                        });
            }));
            return true;
        }

        @Override
        public String toString() {
            return "ReplicationEventHandler{" +
                    "id=" + id + ",connectionClosed=" + nc().connectionClosed() + '}';
        }

        @Override
        public void close() {
            MapReplicationHandler.this.close();
        }
    }


}