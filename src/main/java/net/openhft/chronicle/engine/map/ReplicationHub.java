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
import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.EngineReplication.ModificationIterator;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.map.replication.Bootstrap;
import net.openhft.chronicle.engine.server.internal.MapWireHandler;
import net.openhft.chronicle.engine.server.internal.ReplicationHandler.EventId;
import net.openhft.chronicle.network.connection.AbstractAsyncSubscription;
import net.openhft.chronicle.network.connection.AbstractAsyncTemporarySubscription;
import net.openhft.chronicle.network.connection.AbstractStatelessClient;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.threads.HandlerPriority;
import net.openhft.chronicle.threads.api.EventHandler;
import net.openhft.chronicle.threads.api.EventLoop;
import net.openhft.chronicle.threads.api.InvalidEventHandlerException;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static net.openhft.chronicle.engine.server.internal.ReplicationHandler.EventId.*;

/**
 * Created by Rob Austin
 */
class ReplicationHub extends AbstractStatelessClient {
    private static final Logger LOG = LoggerFactory.getLogger(ChronicleMapKeyValueStore.class);
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
        subscribe(replication, localIdentifier, remoteIdentifier);

        // a non block call to get the identifier from the remote host
        hub.subscribe(new AbstractAsyncSubscription(hub, csp, localIdentifier, "replication " +
                "onConnected") {

            @Override
            public void onSubscribe(@NotNull WireOut wireOut) {
                wireOut.writeEventName(MapWireHandler.EventId.bootstrap).typedMarshallable(bootstrap);
            }

            @Override
            public void onConsumer(@NotNull WireIn inWire) {
                inWire.readDocument(null, d -> {
                    Bootstrap b = d.read(EventId.bootstrap).typedMarshallable();

                    // publishes changes - pushes the replication events
                    try {
                        publish(mi, b);
                    } catch (Exception e) {
                        LOG.error("", e);
                    }
                });
            }

        });

    }

    /**
     * publishes changes - this method pushes the replication events
     *
     * @param mi     the modification iterator that notifies us of changes
     * @param remote details about the remote connection
     */
    private void publish(@NotNull final ModificationIterator mi,
                         @NotNull final Bootstrap remote) {

        final TcpChannelHub hub = this.hub;
        mi.setModificationNotifier(eventLoop::unpause);

        eventLoop.addHandler(new EventHandler() {

            final Bytes bytes = Bytes.elasticByteBuffer();
            final Wire wire = wireType.apply(bytes);

            @Override
            public boolean action() throws InvalidEventHandlerException {

                if (ReplicationHub.this.isClosed.get())
                    throw new InvalidEventHandlerException();

                bytes.clear();

                // publishes single events to free up the event loop, we used to publish all the
                // changes but this can lead to this iterator never completing if updates are
                // coming in from end users that touch these entries
                // the code used to be this
                // hub.lock(() -> mi.forEach(e -> ReplicationHub.this.sendEventAsyncWithoutLock
                //         (replicationEvent, (Consumer<ValueOut>) v -> v.typedMarshallable(e)
                // )));

                // also we have to write the data into a buffer, to free the map lock
                // asap, the old code use to pass the entry to the hub, this was leaving the
                // segment locked and cause deadlocks with the read thread
                mi.nextEntry(e -> wire.writeDocument(false, wireOut ->
                        wireOut.writeEventName(replicationEvent).typedMarshallable(e)));

                ReplicationHub.this.sendBytes(bytes, false);
                return true;
            }

            public HandlerPriority priority() {
                return HandlerPriority.REPLICATION;
            }

        });

        mi.dirtyEntries(remote.lastUpdatedTime());
    }

    /**
     * subscribes to updates
     *
     * @param replication     the event will be applied to the EngineReplication
     * @param localIdentifier our local identifier
     */

    private void subscribe(@NotNull final EngineReplication replication, final byte localIdentifier, final byte remoteIdentifier) {

        // the only has to be a temporary subscription because the onConnected() will be called upon a reconnect
        hub.subscribe(new AbstractAsyncTemporarySubscription(hub, csp, localIdentifier, "replication subscribe") {
            @Override
            public void onSubscribe(@NotNull final WireOut wireOut) {
                wireOut.writeEventName(replicationSubscribe).int8(localIdentifier).writeComment("remoteIdentifier=" + remoteIdentifier);
            }

            @Override
            public void onConsumer(@NotNull final WireIn d) {

                // receives the replication events and applies them
                //noinspection ConstantConditions
                d.readDocument(null, w -> replication.applyReplication(
                        w.read(replicationEvent).typedMarshallable()));
            }

        });

    }

}