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
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.fs.Clusters;
import net.openhft.chronicle.engine.fs.EngineCluster;
import net.openhft.chronicle.engine.tree.HostIdentifier;
import net.openhft.chronicle.network.api.session.SubHandler;
import net.openhft.chronicle.network.cluster.*;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.network.HeaderTcpHandler.HANDLER;
import static net.openhft.chronicle.network.cluster.TerminatorHandler.terminationHandler;

/**
 * Created by Rob Austin
 */
public class UberHandler extends CspTcpHander<EngineWireNetworkContext>
        implements Demarshallable, WriteMarshallable {

    private static final Logger LOG = LoggerFactory.getLogger(UberHandler.class);
    private final int remoteIdentifier;
    private final int localIdentifier;
    private AtomicBoolean isClosing = new AtomicBoolean();
    private ConnectionChangedNotifier connectionChangedNotifier;
    private Asset rootAsset;
    @NotNull
    private String clusterName;
    private int writerIndex;

    @UsedViaReflection
    private UberHandler(WireIn wire) {
        remoteIdentifier = wire.read(() -> "remoteIdentifier").int32();
        localIdentifier = wire.read(() -> "localIdentifier").int32();
        final WireType wireType = wire.read(() -> "wireType").object(WireType.class);
        clusterName = wire.read(() -> "clusterName").text();
        wireType(wireType);
    }

    private UberHandler(int localIdentifier,
                        int remoteIdentifier,
                        @NotNull WireType wireType,
                        @NotNull String clusterName) {

        this.localIdentifier = localIdentifier;
        this.remoteIdentifier = remoteIdentifier;

        assert remoteIdentifier != localIdentifier :
                "remoteIdentifier=" + remoteIdentifier + ", " +
                        "localIdentifier=" + localIdentifier;
        this.clusterName = clusterName;
        wireType(wireType);
    }

    private static WriteMarshallable uberHandler(final WriteMarshallable m) {
        return wire -> {
            try (final DocumentContext dc = wire.writingDocument(true)) {
                wire.write(() -> HANDLER).typedMarshallable(m);
            }
        };
    }

    public boolean isClosed() {
        return isClosing.get();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(() -> "remoteIdentifier").int32(localIdentifier);
        wire.write(() -> "localIdentifier").int32(remoteIdentifier);
        final WireType value = wireType();
        wire.write(() -> "wireType").object(value);
        wire.write(() -> "clusterName").text(clusterName);
    }

    @Override
    protected void onInitialize() {
        EngineWireNetworkContext nc = nc();
        nc.wireType(wireType());
        isAcceptor(nc.isAcceptor());
        rootAsset = nc.rootAsset();

        assert checkIdentifierEqualsHostId();
        assert remoteIdentifier != localIdentifier :
                "remoteIdentifier=" + remoteIdentifier + ", " +
                        "localIdentifier=" + localIdentifier;

        final WireOutPublisher publisher = nc.wireOutPublisher();
        publisher(publisher);

        EventLoop eventLoop = rootAsset.findOrCreateView(EventLoop.class);
        eventLoop.start();

        final Clusters clusters = rootAsset.findView(Clusters.class);

        final EngineCluster engineCluster = clusters.get(clusterName);
        if (engineCluster == null) {
            Jvm.warn().on(getClass(), "cluster=" + clusterName, new RuntimeException("cluster  not " +
                    "found, cluster=" + clusterName));
            return;
        }

        // note : we have to publish the uber handler, even if we send a termination event
        // this is so the termination event can be processed by the receiver
        if (nc().isAcceptor())
            // reflect the uber handler
            publish(uberHandler());

        nc.terminationEventHandler(engineCluster.findTerminationEventHandler(remoteIdentifier));

        if (!checkConnectionStrategy(engineCluster)) {
            // the strategy has told us to reject this connection, we have to first notify the
            // other host, we will do this by sending a termination event
            publish(terminationHandler(localIdentifier, remoteIdentifier, nc.newCid()));
            closeSoon();
            return;
        }

        if (!isClosing.get())
            notifyConnectionListeners(engineCluster);
    }

    private boolean checkIdentifierEqualsHostId() {
        final HostIdentifier hostIdentifier = rootAsset.findOrCreateView(HostIdentifier.class);
        return hostIdentifier == null || localIdentifier == hostIdentifier.hostId();
    }

    private void notifyConnectionListeners(EngineCluster cluster) {
        connectionChangedNotifier = cluster.findClusterNotifier(remoteIdentifier);
        if (connectionChangedNotifier != null)
            connectionChangedNotifier.onConnectionChanged(true, nc());
    }

    private boolean checkConnectionStrategy(@NotNull EngineCluster cluster) {
        final ConnectionStrategy strategy = cluster.findConnectionStrategy(remoteIdentifier);
        return strategy == null ||
                strategy.notifyConnected(this, localIdentifier, remoteIdentifier);
    }

    private WriteMarshallable uberHandler() {
        final UberHandler handler = new UberHandler(
                localIdentifier,
                remoteIdentifier,
                wireType(),
                clusterName);
        return uberHandler(handler);
    }

    /**
     * wait 2 seconds before closing the socket connection, this should allow time of the
     * termination event to be sent.
     */
    private void closeSoon() {
        isClosing.set(true);
        ScheduledExecutorService closer = newSingleThreadScheduledExecutor(new NamedThreadFactory("closer", true));
        closer.schedule(() -> {
            closer.shutdown();
            close();
        }, 2, SECONDS);
    }

    @Override
    public void close() {
        if (!isClosing.getAndSet(true) && connectionChangedNotifier != null)
            connectionChangedNotifier.onConnectionChanged(false, nc());

        super.close();
    }

    @Override
    protected void onRead(@NotNull DocumentContext dc, @NotNull WireOut outWire) {
        if (isClosing.get())
            return;

        onMessageReceivedOrWritten();

        Wire inWire = dc.wire();
        if (dc.isMetaData()) {
            if (!readMeta(inWire))
                return;

            SubHandler handler = handler();
            handler.remoteIdentifier(remoteIdentifier);
            handler.localIdentifier(localIdentifier);
            handler.onInitialize(outWire);
            return;
        }

        SubHandler handler = handler();
        if (handler == null)
            throw new IllegalStateException("handler == null, check that the " +
                    "Csp/Cid has been sent, failed to " +
                    "fully " +
                    "process the following " +
                    "YAML\n");

        if (dc.isData() && !inWire.bytes().isEmpty())
            handler.onRead(inWire, outWire);
    }

    @Override
    protected void onBytesWritten() {
        onMessageReceivedOrWritten();
    }

    /**
     * ready to accept wire
     *
     * @param outWire the wire that you wish to write
     */
    @Override
    protected void onWrite(@NotNull WireOut outWire) {
        SubHandler handler = handler();
        if (handler != null)
            handler.onWrite(outWire);

        for (int i = 0; i < writers.size(); i++) {

            if (isClosing.get())
                return;

            WriteMarshallable w = next();
            if (w != null)
                w.writeMarshallable(outWire);
        }
    }

    /**
     * round robbins - the writers, we should only write when the buffer is empty, as // we can't
     * guarantee that we will have enough space to add more data to the out wire.
     *
     * @return the  Marshallable that you are writing to
     */
    private WriteMarshallable next() {
        if (writerIndex >= writers.size())
            writerIndex = 0;
        return writers.get(writerIndex++);
    }

    private void onMessageReceivedOrWritten() {
        final HeartbeatEventHandler heartbeatEventHandler = heartbeatEventHandler();

        if (heartbeatEventHandler != null)
            heartbeatEventHandler.onMessageReceived();
    }

    public static class Factory implements BiFunction<ClusterContext, HostDetails,
            WriteMarshallable>, Demarshallable {

        @UsedViaReflection
        private Factory(@NotNull WireIn wireIn) {
        }

        public Factory() {
        }

        @Override
        public WriteMarshallable apply(@NotNull final ClusterContext clusterContext,
                                       @NotNull final HostDetails hostdetails) {
            final byte localIdentifier = clusterContext.localIdentifier();
            final int remoteIdentifier = hostdetails.hostId();
            final WireType wireType = clusterContext.wireType();
            final String name = clusterContext.clusterName();
            return uberHandler(new UberHandler(localIdentifier, remoteIdentifier, wireType, name));
        }
    }
}
