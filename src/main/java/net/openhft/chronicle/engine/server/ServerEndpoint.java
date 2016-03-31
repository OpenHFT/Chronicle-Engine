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
package net.openhft.chronicle.engine.server;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.map.ChronicleMapKeyValueStore;
import net.openhft.chronicle.engine.server.internal.EngineWireHandler;
import net.openhft.chronicle.engine.server.internal.EngineWireNetworkContext;
import net.openhft.chronicle.network.*;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.connection.VanillaWireOutPublisher;
import net.openhft.chronicle.threads.Threads;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

/**
 * Created by Rob Austin
 */
public class ServerEndpoint implements Closeable {

    public static final int HEARTBEAT_INTERVAL_TICKS = Integer.getInteger("heartbeat.interval.ticks", 500);
    public static final int HEARTBEAT_TIME_OUT_TICKS = Integer.getInteger("heartbeat.timeout.ticks", 10_000);
    private static final Logger LOG = LoggerFactory.getLogger(ChronicleMapKeyValueStore.class);

    @Nullable
    private final EventLoop eg;

    @NotNull
    private final AtomicBoolean isClosed = new AtomicBoolean();

    @Nullable
    private AcceptorEventHandler eah;


    public ServerEndpoint(@NotNull String hostPortDescription,
                          @NotNull AssetTree assetTree) {

        eg = assetTree.root().acquireView(EventLoop.class);
        Threads.withThreadGroup(assetTree.root().getView(ThreadGroup.class), () -> {
            start(hostPortDescription, assetTree);
            return null;
        });

        assetTree.root().addView(ServerEndpoint.class, this);
    }


    @Nullable
    private AcceptorEventHandler start(@NotNull String hostPortDescription,
                                       @NotNull final AssetTree assetTree) throws IOException {
        assert eg != null;

        eg.start();
        if (LOG.isInfoEnabled())
            LOG.info("starting server=" + hostPortDescription);

        final EventLoop eventLoop = assetTree.root().findOrCreateView(EventLoop.class);
        assert eventLoop != null;

        Function<NetworkContext, TcpEventHandler> networkContextTcpEventHandlerFunction = (networkContext) -> {

            try {
                final EngineWireNetworkContext nc = (EngineWireNetworkContext) networkContext;
                if (nc.isAcceptor())
                    nc.wireOutPublisher(new VanillaWireOutPublisher(WireType.TEXT));
                final TcpEventHandler handler = new TcpEventHandler(networkContext);

                final Function<Object, TcpHandler> consumer = o -> {
                    if (o instanceof SessionDetailsProvider) {
                        final SessionDetailsProvider sessionDetails = (SessionDetailsProvider) o;
                        //      nc.heartbeatIntervalTicks(heartbeatIntervalTicks());
                        //    nc.heartBeatTimeoutTicks(heartbeatTimeoutMs());
                        nc.sessionDetails(sessionDetails);
                        nc.wireType(sessionDetails.wireType());
                        final WireType wireType = nc.sessionDetails().wireType();
                        if (wireType != null)
                            nc.wireOutPublisher().wireType(wireType);
                        return new EngineWireHandler();
                    } else if (o instanceof TcpHandler)
                        return (TcpHandler) o;

                    throw new UnsupportedOperationException("not supported class=" + o.getClass());
                };

                final Function<EngineWireNetworkContext, TcpHandler> f
                        = x -> new HeaderTcpHandler<>(handler, consumer, x);

                final WireTypeSniffingTcpHandler sniffer = new
                        WireTypeSniffingTcpHandler<>(handler, nc, f);

                handler.tcpHandler(sniffer);
                return handler;

            } catch (IOException e) {
                throw Jvm.rethrow(e);
            }
        };
        final AcceptorEventHandler eah = new AcceptorEventHandler(
                hostPortDescription,
                networkContextTcpEventHandlerFunction,
                () -> new EngineWireNetworkContext(assetTree.root()));

        eg.addHandler(eah);
        this.eah = eah;
        return eah;
    }


    @Override
    public void close() {
        isClosed.set(true);
        closeQuietly(eah);
        eah = null;
    }
}
