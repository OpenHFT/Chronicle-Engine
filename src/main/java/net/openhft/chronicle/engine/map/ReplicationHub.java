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

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.EngineReplication.ModificationIterator;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.map.CMap2EngineReplicator.VanillaReplicatedEntry;
import net.openhft.chronicle.engine.map.replication.Bootstrap;
import net.openhft.chronicle.engine.server.internal.MapWireHandler;
import net.openhft.chronicle.engine.server.internal.ReplicationHandler2.EventId;
import net.openhft.chronicle.network.connection.*;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import static net.openhft.chronicle.engine.server.internal.ReplicationHandler2.EventId.*;

/**
 * Created by Rob Austin
 */
class ReplicationHub extends AbstractStatelessClient {
    private static final Logger LOG = LoggerFactory.getLogger(ChronicleMapKeyValueStore.class);
    final ThreadLocal<VanillaReplicatedEntry> vre = ThreadLocal.withInitial(VanillaReplicatedEntry::new);
    private final EventLoop eventLoop;
    private final AtomicBoolean isClosed;
    private final Function<Bytes, Wire> wireType;

    public ReplicationHub(@NotNull RequestContext context,
                          @NotNull final TcpChannelHub hub,
                          @NotNull EventLoop eventLoop,
                          @NotNull AtomicBoolean isClosed,
                          @NotNull Function<Bytes, Wire> wireType) {
        super(hub, (long) 0, toUri(context));

        this.eventLoop = eventLoop;
        this.isClosed = isClosed;
        this.wireType = wireType;
    }

    private static String toUri(@NotNull final RequestContext context) {
        final StringBuilder uri = new StringBuilder(context.fullName()
                + "?view=" + "Replication");

        if (context.keyType() != String.class)
            uri.append("&keyType=").append(context.keyType().getName());

        if (context.valueType() != String.class)
            uri.append("&valueType=").append(context.valueType().getName());

        return uri.toString();
    }

    public void bootstrap(@NotNull EngineReplication replication,
                          byte localIdentifier,
                          byte remoteIdentifier) {

        // a non block call to get the identifier from the remote host
        hub.subscribe(new AbstractAsyncSubscription(hub, csp, localIdentifier, "ReplicationHub bootstrap") {
            @Override
            public void onSubscribe(@NotNull WireOut wireOut) {

                if (LOG.isDebugEnabled())
                    LOG.debug("onSubscribe - localIdentifier=" + localIdentifier + "," +
                            "remoteIdentifier=" + remoteIdentifier);

                wireOut.writeEventName(identifier)
                        .marshallable(WriteMarshallable.EMPTY)
                        .writeComment(toString() + ", tcpChannelHub={" + hub.toString() + "}");
            }

            @Override
            public void onConsumer(@NotNull WireIn inWire) {
                if (Jvm.isDebug())
                    LOG.info("client : bootstrap");

                inWire.readDocument(null, d -> {
                    byte remoteIdentifier = d.read(identifierReply).int8();
                    onConnected(localIdentifier, remoteIdentifier, replication);
                });
            }

            @NotNull
            @Override
            public String toString() {
                return "bootstrap {localIdentifier=" + localIdentifier + " ,remoteIdentifier=" + remoteIdentifier + "}";
            }
        });
    }

    /**
     * called when the connection is established to the remote host, if the connection to the remote
     * host is lost and re-established this method is called again each time the connection is
     * establish.
     *
     * @param localIdentifier  the identifier of this host
     * @param remoteIdentifier the identifier of the remote host
     * @param replication      the instance the handles the replication
     */
    private void onConnected(final byte localIdentifier, byte remoteIdentifier, @NotNull EngineReplication replication) {
        final ModificationIterator mi = replication.acquireModificationIterator(remoteIdentifier);
        assert mi != null;
        final long lastModificationTime = replication.lastModificationTime(remoteIdentifier);

        final Bootstrap bootstrap = new Bootstrap();
        bootstrap.lastUpdatedTime(lastModificationTime);
        bootstrap.identifier(localIdentifier);

        // subscribes to updates - receives the replication events
        //  subscribe(replication, localIdentifier, remoteIdentifier);

        // a non block call to get the identifier from the remote host
        hub.subscribe(new AbstractAsyncTemporarySubscription(hub, csp, localIdentifier, "replication " +
                              "onConnected") {

                          int count = 0;

                          @Override
                          public void onSubscribe(@NotNull WireOut wireOut) {
                              wireOut.writeEventName(MapWireHandler.EventId.bootstrap).typedMarshallable(bootstrap);
                          }

                          @Override
                          public void onConsumer(@NotNull WireIn inWire) {
                              if (Jvm.isDebug())
                                  LOG.info("client : onConsumer - publishing updates");

                              inWire.readDocument(null, d -> {

                                  StringBuilder eventName = Wires.acquireStringBuilder();

                                  final ValueIn valueIn = d.readEventName(eventName);

                                  if (EventId.bootstrap.contentEquals(eventName)) {
                                      Bootstrap b = valueIn.typedMarshallable();

                                      // publishes changes - pushes the replication events
                                      try {
                                          publish(mi, b, remoteIdentifier);

                                      } catch (RuntimeException e) {
                                          LOG.warn("", e);
                                      }
                                      return;

                                  }
                                  if (replicationEvent.contentEquals(eventName)) {
                                      final VanillaReplicatedEntry replicatedEntry = vre.get();
                                      valueIn.marshallable(replicatedEntry);

                                      if (LOG.isInfoEnabled()) {
                                          long delay = System.currentTimeMillis() - replicatedEntry.timestamp();
                                          if (delay > 100) {
                                              LOG.info("Rcv Clt latency=" + delay + "ms\t");
                                              if (count++ % 10 == 0) {
                                                  LOG.info("");
                                              }
                                          }
                                      }

                                      replication.applyReplication(replicatedEntry);
                                  }

                                  // receives replication events
                                  else if (CoreFields.lastUpdateTime.contentEquals(eventName)) {

                                      if (Jvm.isDebug())
                                          System.out.println("server : received lastUpdateTime");

                                      final long time = valueIn.int64();
                                      final byte id = d.read(() -> "id").int8();

                                      replication.setLastModificationTime(id, time);

                                  }

                              });
                          }
                      }
        );
    }

    /**
     * publishes changes - this method pushes the replication events
     *
     * @param mi               the modification iterator that notifies us of changes
     * @param remote           details about the remote connection
     * @param remoteIdentifier the identifier of the remote host
     */
    void publish(@NotNull final ModificationIterator mi,
                 @NotNull final Bootstrap remote, byte remoteIdentifier) {

        final TcpChannelHub hub = this.hub;
        mi.setModificationNotifier(eventLoop::unpause);

        eventLoop.addHandler(true, new RepEventHandler(hub, mi, remoteIdentifier));

        mi.dirtyEntries(remote.lastUpdatedTime());
    }

    private class RepEventHandler implements EventHandler, Consumer<EngineReplication.ReplicationEntry> {

        final Bytes bytes;
        final Wire wire;
        private final TcpChannelHub hub;
        private final ModificationIterator mi;
        private final byte remoteIdentifier;
        boolean hasSentLastUpdateTime;
        long lastUpdateTime;
        boolean hasLogged;

        public RepEventHandler(TcpChannelHub hub, ModificationIterator mi, byte remoteIdentifier) {
            this.hub = hub;
            this.mi = mi;
            this.remoteIdentifier = remoteIdentifier;
            bytes = Bytes.elasticByteBuffer();
            wire = wireType.apply(bytes);
            hasSentLastUpdateTime = false;
            lastUpdateTime = 0;
        }

        @Override
        public boolean action() throws InvalidEventHandlerException {

            if (hub.isOutBytesLocked())
                return false;

            if (!hub.isOutBytesEmpty())
                return false;

            if (ReplicationHub.this.isClosed.get())
                throw new InvalidEventHandlerException();

            bytes.clear();

            if (!mi.hasNext()) {

                // because events arrive in a bitset ( aka random ) order ( not necessary in
                // time order ) we can only be assured that the latest time of
                // the last event is really the latest time, once all the events
                // have been received, we know when we have received all events
                // when there are no more events to process.
                if (!hasSentLastUpdateTime && lastUpdateTime > 0) {
                    wire.writeNotCompleteDocument(false,
                            wire -> {
                                wire.writeEventName(CoreFields.lastUpdateTime).int64(lastUpdateTime);
                                wire.write(() -> "id").int8(remoteIdentifier);
                            }
                    );

                    hasSentLastUpdateTime = true;

                    if (!hasLogged)
                        hasLogged = true;

                    if (bytes.readRemaining() > 0) {
                        ReplicationHub.this.sendBytes(bytes, false);
                        return true;
                    }

                    return false;
                }
            }

            // publishes single events to free up the event loop, we used to publish all the
            // changes but this can lead to this iterator never completing if updates are
            // coming in from end users that touch these entries
            // the code used to be this
            // hub.lock(() -> mi.forEach(e -> ReplicationHub.this.sendEventAsyncWithoutLock
            //         (replicationEvent, (WriteValue) v -> v.typedMarshallable(e)
            // )));

            // also we have to write the data into a buffer, to free the map lock
            // asap, the old code use to pass the entry to the hub, this was leaving the
            // segment locked and cause deadlocks with the read thread
            mi.nextEntry(this);

            if (bytes.readRemaining() > 0) {
                ReplicationHub.this.sendBytes(bytes, false);
                return true;
            }

            return false;
        }

        @Override
        public void accept(EngineReplication.ReplicationEntry e) {
            long updateTime = Math.max(lastUpdateTime, e.timestamp());
            if (updateTime > lastUpdateTime) {
                hasSentLastUpdateTime = false;
                lastUpdateTime = updateTime;
            }

            if (Jvm.isDebug() && LOG.isDebugEnabled()) {
                long delay = System.currentTimeMillis() - e.timestamp();
                LOG.debug("*****\t\t\t\tSENT : CLIENT :replicatedEntry latency=" +
                        delay + "ms");
            }

            wire.writeNotCompleteDocument(false, wireOut ->
                    wireOut.writeEventName(replicationEvent).typedMarshallable(e));
        }

        public HandlerPriority priority() {
            return HandlerPriority.REPLICATION;
        }
    }
}