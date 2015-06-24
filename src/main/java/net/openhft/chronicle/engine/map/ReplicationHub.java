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
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.api.tree.View;
import net.openhft.chronicle.engine.map.replication.Bootstrap;
import net.openhft.chronicle.network.connection.AbstractStatelessClient;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import static net.openhft.chronicle.engine.collection.CollectionWireHandler.SetEventId.identifier;
import static net.openhft.chronicle.engine.server.internal.MapWireHandler.EventId.bootstap;
import static net.openhft.chronicle.engine.server.internal.MapWireHandler.EventId.replicationEvent;
import static net.openhft.chronicle.network.connection.CoreFields.reply;

/**
 * Created by Rob Austin
 */
public class ReplicationHub extends AbstractStatelessClient implements View {
    private static final Logger LOG = LoggerFactory.getLogger(ChronicleMapKeyValueStore.class);

    public ReplicationHub(RequestContext context, @NotNull final TcpChannelHub hub) {
        super(hub, (long) 0, toUri(context));
    }


    private static String toUri(final RequestContext context) {
        final StringBuilder uri = new StringBuilder("/" + context.name()
                + "?view=" + "Replication");

        if (context.keyType() != String.class)
            uri.append("&keyType=").append(context.keyType().getName());

        if (context.valueType() != String.class)
            uri.append("&valueType=").append(context.valueType().getName());

        return uri.toString();
    }

    public void bootstrap(EngineReplication replication, int localIdentifer) throws InterruptedException {

        final byte remoteIdentifier = proxyReturnByte(identifier);
        final ModificationIterator mi = replication.acquireModificationIterator(remoteIdentifier);
        final long lastModificationTime = replication.lastModificationTime(remoteIdentifier);

        final Bootstrap bootstrap = new Bootstrap();
        bootstrap.lastUpdatedTime(lastModificationTime);
        bootstrap.identifier((byte) localIdentifer);

        final AtomicLong tid = new AtomicLong();

        final Function<ValueIn, Bootstrap> typedMarshallable = ValueIn::typedMarshallable;
        final Consumer<ValueOut> valueOutConsumer = o -> o.typedMarshallable(bootstrap);

        final Bootstrap b = (Bootstrap) proxyReturnWireConsumerInOut(
                bootstap, reply, valueOutConsumer, typedMarshallable, tid::set);

        mi.dirtyEntries(b.lastUpdatedTime());

        try {

            // send replication events
            this.hub.outBytesLock().lock();
            try {
                mi.forEach(e -> this.hub.outWire().writeDocument(false, wireOut ->
                        wireOut.writeEventName(replicationEvent).marshallable(e)));

            // receives replication events
            this.hub.asyncReadSocket(tid.get(), d ->
                    d.readDocument(null, w -> replication.applyReplication(
                            w.read(reply).typedMarshallable())));

            this.hub.writeSocket(this.hub.outWire());
            } finally {
                this.hub.outBytesLock().unlock();
            }

        } catch (Throwable t) {
            LOG.error("", t);
        }

    }

}