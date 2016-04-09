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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.engine.api.EngineReplication.ModificationIterator;
import net.openhft.chronicle.engine.api.pubsub.Replication;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.map.CMap2EngineReplicator.VanillaReplicatedEntry;
import net.openhft.chronicle.engine.map.replication.Bootstrap;
import net.openhft.chronicle.engine.tree.HostIdentifier;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;

import static net.openhft.chronicle.engine.server.internal.ReplicationHandler2.EventId.*;

/**
 * Created by Rob Austin
 */
public class ReplicationHandler2<E> extends AbstractHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicationHandler2.class);
    private final StringBuilder eventName = new StringBuilder();
    private Replication replication;
    private WireOutPublisher publisher;
    private HostIdentifier hostId;
    private long tid;
    private boolean isAcceptor;
    private EventLoop eventLoop;

    private byte remoteIdentifier;
    private byte localIdentifier;
    private RequestContext requestContext;
    private long cid;
    @NotNull
    private final BiConsumer<WireIn, Long> dataConsumer = new BiConsumer<WireIn, Long>() {

        final ThreadLocal<VanillaReplicatedEntry> vre = ThreadLocal.withInitial(VanillaReplicatedEntry::new);

        @Override
        public void accept(@NotNull final WireIn inWire, Long inputTid) {

            eventName.setLength(0);
            final ValueIn valueIn = inWire.readEventName(eventName);

            // receives replication events
            if (CoreFields.lastUpdateTime.contentEquals(eventName)) {
                if (Jvm.isDebug())
                    LOG.info("server : received lastUpdateTime");
                final long time = valueIn.int64();
                final byte id = inWire.read(() -> "id").int8();
                replication.setLastModificationTime(id, time);
                return;
            }

            // receives replication events
            if (replicationEvent.contentEquals(eventName)) {
                if (Jvm.isDebug() && LOG.isDebugEnabled())
                    LOG.debug("server : received replicationEvent");
                VanillaReplicatedEntry replicatedEntry = vre.get();
                valueIn.marshallable(replicatedEntry);

                if (Jvm.isDebug() && LOG.isDebugEnabled())
                    LOG.debug("*****\t\t\t\t ->  RECEIVED : SERVER : replication latency=" + (System
                            .currentTimeMillis() - replicatedEntry.timestamp()) + "ms  ");

                replication.applyReplication(replicatedEntry);
                return;
            }

            assert outWire != null;

            if (bootstrap.contentEquals(eventName)) {

                final String name = Thread.currentThread().getName();

                // receive bootstrap
                final long timestamp = valueIn.int64();

                try {
                    assert localIdentifier != remoteIdentifier;
                } catch (Error e) {
                    e.printStackTrace();
                }

                final ModificationIterator mi = replication.acquireModificationIterator(remoteIdentifier);
                if (mi != null)
                    mi.dirtyEntries(timestamp);

                if (isAcceptor) {

                    outWire.writeDocument(true, d -> {
                        final String fullName = requestContext.fullName();
                        outWire.write(CoreFields.csp).text(fullName + "?view=Replication")
                                .write(CoreFields.cid).int64(cid);
                    });

                    outWire.writeDocument(false, d -> outWire.write(bootstrap)
                            .int64(replication.lastModificationTime(remoteIdentifier))
                            .writeComment("localIdentifier=" + hostId.hostId() +
                                    ",remoteIdentifier=" + remoteIdentifier));

                    logYaml();
                }

                if (Jvm.isDebug())
                    LOG.info("server : received simplebootstrap");
                if (mi == null)
                    return;

                // sends replication events back to the remote client
                mi.setModificationNotifier(eventLoop::unpause);

                eventLoop.addHandler(true, new ReplicationEventHandler(mi, remoteIdentifier, inputTid));
                return;
            }

            outWire.writeDocument(true, wire -> outWire.writeEventName(CoreFields.tid).int64(tid));

            if (identifier.contentEquals(eventName))
                writeData(inWire.bytes(), out -> outWire.write(identifierReply).int8(hostId.hostId()));

            if (bootstrap.contentEquals(eventName)) {
                writeData(true, inWire.bytes(), out -> {
                    if (LOG.isDebugEnabled())
                        LOG.debug("server : received bootstrap request");

                    // receive bootstrap
                    final Bootstrap inBootstrap = valueIn.typedMarshallable();
                    if (inBootstrap == null)
                        return;
                    final byte id = inBootstrap.identifier();

                    final ModificationIterator mi = replication.acquireModificationIterator(id);
                    if (mi != null)
                        mi.dirtyEntries(inBootstrap.lastUpdatedTime());

                    // send bootstrap
                    final Bootstrap outBootstrap = new Bootstrap();
                    outBootstrap.identifier(hostId.hostId());
                    outBootstrap.lastUpdatedTime(replication.lastModificationTime(id));
                    outWire.writeEventName(bootstrap).typedMarshallable(outBootstrap);

                    if (Jvm.isDebug())
                        LOG.info("server : received replicationSubscribe");

                    // receive bootstrap
                    if (mi == null)
                        return;
                    // sends replication events back to the remote client
                    mi.setModificationNotifier(eventLoop::unpause);

                    eventLoop.addHandler(true, new ReplicationEventHandler(mi, id, inputTid));
                });
            }

        }
    };

    void process(@NotNull final WireIn inWire,
                 final WireOutPublisher publisher,
                 final long tid,
                 final Wire outWire,
                 final HostIdentifier hostId,
                 final Replication replication,
                 final EventLoop eventLoop,
                 final boolean isServerSocket,
                 byte remoteIdentifier,
                 final RequestContext requestContext,
                 long cid,
                 byte localIdentifier) {

        this.eventLoop = eventLoop;
        this.isAcceptor = isServerSocket;
        setOutWire(outWire);

        this.localIdentifier = localIdentifier;
        this.hostId = hostId;
        this.publisher = publisher;
        this.replication = replication;
        this.tid = tid;
        this.remoteIdentifier = remoteIdentifier;
        this.requestContext = requestContext;
        this.cid = cid;
        dataConsumer.accept(inWire, tid);

    }

    public enum EventId implements ParameterizeWireKey {
        publish,
        onEndOfSubscription,
        apply,
        replicationEvent,
        identifierReply,
        bootstrap,
        identifier;

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

    private class ReplicationEventHandler implements EventHandler {

        private final ModificationIterator mi;
        private final byte id;
        boolean hasSentLastUpdateTime;
        long lastUpdateTime;
        boolean hasLogged;
        int count;
        long startBufferFullTimeStamp;

        public ReplicationEventHandler(ModificationIterator mi, byte id, Long inputTid) {
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
            if (connectionClosed)
                throw new InvalidEventHandlerException();

            final WireOutPublisher publisher = ReplicationHandler2.this.publisher;

            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (publisher) {
                // given the sending an event to the publish hold the chronicle map lock
                // we will send only one at a time

                if (!publisher.canTakeMoreData()) {
                    if (startBufferFullTimeStamp == 0) {
                        startBufferFullTimeStamp = System.currentTimeMillis();
                    }
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
                            w.writeDocument(true, d -> d.write(CoreFields.cid).int64(cid));
                            w.writeNotReadyDocument(false, d -> {
                                        d.writeEventName(CoreFields.lastUpdateTime).int64(lastUpdateTime);
                                        d.write(() -> "id").int8(id);
                                    }
                            );
                        });

                        hasSentLastUpdateTime = true;

                        if (!hasLogged) {
                            LOG.info("received ALL replication the EVENTS for " +
                                    "id=" + id);
                            hasLogged = true;
                        }

                    }
                    return false;
                }

                mi.nextEntry(e -> publisher.put(null, w -> {

                    if (e.remoteIdentifier() == hostId.hostId())
                        return;

                    long newlastUpdateTime = Math.max(lastUpdateTime, e.timestamp());

                    if (newlastUpdateTime > lastUpdateTime) {
                        hasSentLastUpdateTime = false;
                        lastUpdateTime = newlastUpdateTime;
                    }

                    if (LOG.isDebugEnabled())
                        LOG.debug("publish from server response from iterator " +
                                "localIdentifier=" + hostId + " ,remoteIdentifier=" +
                                id + " event=" + e);

                    w.writeDocument(true, d -> d.write(CoreFields.cid).int64(cid));
                    w.writeNotReadyDocument(false,
                            d -> d.writeEventName(replicationEvent).typedMarshallable(e));

                }));
            }
            return true;
        }

        @Override
        public String toString() {
            return "ReplicationEventHandler{" +
                    "id=" + id + ",connectionClosed=" + connectionClosed +
                    '}';
        }
    }
}
