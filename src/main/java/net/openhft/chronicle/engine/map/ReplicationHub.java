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

import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.EngineReplication.ModificationIterator;
import net.openhft.chronicle.engine.map.replication.Bootstrap;
import net.openhft.chronicle.network.connection.AbstractStatelessClient;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import static net.openhft.chronicle.engine.collection.CollectionWireHandler.SetEventId.identifier;
import static net.openhft.chronicle.engine.server.internal.MapWireHandler.EventId.bootstap;
import static net.openhft.chronicle.engine.server.internal.MapWireHandler.EventId.replicationEvent;
import static net.openhft.chronicle.wire.CoreFields.reply;

/**
 * Created by Rob Austin
 */
public class ReplicationHub extends AbstractStatelessClient {
    private static final Logger LOG = LoggerFactory.getLogger(ChronicleMapKeyValueStore.class);

    public ReplicationHub(@NotNull final TcpChannelHub hub,
                          @NotNull final String csp) {
        super(hub, 0, csp);
    }

    public void bootstrap(EngineReplication replication, Executor eventLoop, byte localIdentifier)
            throws
            InterruptedException {

        final byte remoteIdentifier = proxyReturnByte(identifier);
        final ModificationIterator mi = replication.acquireModificationIterator(remoteIdentifier);
        final long lastModificationTime = replication.lastModificationTime(remoteIdentifier);

        final Bootstrap bootstrap = new Bootstrap();
        bootstrap.lastUpdatedTime(lastModificationTime);
        bootstrap.identifier(localIdentifier);

        final AtomicLong tid = new AtomicLong();

        final Function<ValueIn, Bootstrap> typedMarshallable = ValueIn::typedMarshallable;
        final Consumer<ValueOut> valueOutConsumer = o -> o.typedMarshallable(bootstrap);

        final Bootstrap inBootstrap = (Bootstrap) proxyReturnWireConsumerInOut(
                bootstap, reply, valueOutConsumer, typedMarshallable, tid::set);

        mi.dirtyEntries(inBootstrap.lastUpdatedTime());

        eventLoop.execute(() -> {

            try {

                // send replication events
                hub.outBytesLock().lock();
                try {
                    mi.forEach(e -> hub.outWire().writeDocument(false, wireOut ->
                            wireOut.writeEventName(replicationEvent).marshallable(e)));
                } finally {
                    hub.outBytesLock().unlock();
                }

                // receives replication events
                hub.asyncReadSocket(tid.get(), d ->
                        d.readDocument(null, w -> replication.applyReplication(
                                w.read(reply).typedMarshallable())));

            } catch (Throwable t) {
                LOG.error("", t);
            }
        });

    }

}