/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package net.openhft.chronicle.engine.server;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.engine.api.tree.AssetTree;
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

    private static final Logger LOG = LoggerFactory.getLogger(ServerEndpoint.class);

    @Nullable
    private final EventLoop eg;

    @NotNull
    private final AtomicBoolean isClosed = new AtomicBoolean();

    @Nullable
    private AcceptorEventHandler eah;

    public ServerEndpoint(@NotNull String hostPortDescription,
                          @NotNull AssetTree assetTree,
                          @NotNull NetworkStatsListener networkStatsListener) throws IOException {

        eg = assetTree.root().acquireView(EventLoop.class);
        Threads.<Void, IOException>withThreadGroup(assetTree.root().getView(ThreadGroup.class), () -> {
            start(hostPortDescription, assetTree, networkStatsListener);
            return null;
        });

        assetTree.root().addView(ServerEndpoint.class, this);
    }

    public ServerEndpoint(@NotNull String hostPortDescription,
                          @NotNull AssetTree assetTree) throws IOException {
        this(hostPortDescription, assetTree, new NetworkStatsListener() {

            @Override
            public void close() {
                LOG.info(" host=" + host + ", port=" + port + ", isConnected=false");
            }

            private String host;
            private long port;

            @Override
            public void networkContext(NetworkContext networkContext) {

            }

            @Override
            public void onNetworkStats(long writeBps, long readBps, long socketPollCountPerSecond) {
                LOG.info("writeKBps=" + writeBps / 1000 + ", readKBps=" + readBps / 1000 +
                        ", socketPollCountPerSecond=" + socketPollCountPerSecond / 1000 + "K, " +
                        "host=" + host + ", port=" + port + ", isConnected=true");
            }

            @Override
            public void onHostPort(String hostName, int port) {
                host = hostName;
                this.port = port;
            }
        });
    }


    @Nullable
    private AcceptorEventHandler start(@NotNull String hostPortDescription,
                                       @NotNull final AssetTree assetTree,
                                       @NotNull NetworkStatsListener networkStatsListener)
            throws IOException {
        assert eg != null;

        eg.start();
        if (LOG.isInfoEnabled())
            LOG.info("starting server=" + hostPortDescription);

        final EventLoop eventLoop = assetTree.root().findOrCreateView(EventLoop.class);
        assert eventLoop != null;

/*
        if (networkStatsListener != null) {
            if (sc.socket() != null && sc.socket().getRemoteSocketAddress()
                    instanceof InetSocketAddress)
                networkStatsListener.onHostPort(
                        ((InetSocketAddress) sc.socket().getRemoteSocketAddress()).getHostName(),
                        ((InetSocketAddress) sc.socket().getRemoteSocketAddress()).getPort());
        }
*/


        Function<NetworkContext, TcpEventHandler> networkContextTcpEventHandlerFunction = (networkContext) -> {
            final EngineWireNetworkContext nc = (EngineWireNetworkContext) networkContext;
            if (nc.isAcceptor())
                nc.wireOutPublisher(new VanillaWireOutPublisher(WireType.TEXT));
            final TcpEventHandler handler = new TcpEventHandler(networkContext);

            final Function<Object, TcpHandler> consumer = o -> {
                if (o instanceof SessionDetailsProvider) {
                    final SessionDetailsProvider sessionDetails = (SessionDetailsProvider) o;
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
        };
        final AcceptorEventHandler eah = new AcceptorEventHandler(
                hostPortDescription,
                networkContextTcpEventHandlerFunction,
                () -> createNetworkContext(assetTree, networkStatsListener));

        eg.addHandler(eah);
        this.eah = eah;
        return eah;
    }

    private EngineWireNetworkContext createNetworkContext(AssetTree assetTree,
                                                          final NetworkStatsListener networkStatsListener) {
        final EngineWireNetworkContext nc = new EngineWireNetworkContext(assetTree.root());
        nc.networkStatsListener(networkStatsListener);
        networkStatsListener.networkContext(nc);
        return nc;
    }

    @Override
    public void close() {
        isClosed.set(true);
        closeQuietly(eah);
        eah = null;
    }
}
