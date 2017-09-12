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
import net.openhft.chronicle.engine.server.internal.EngineWireNetworkContext;
import net.openhft.chronicle.network.AcceptorEventHandler;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.NetworkStatsListener;
import net.openhft.chronicle.network.TcpEventHandler;
import net.openhft.chronicle.threads.Threads;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

/*
 * Created by Rob Austin
 */
public class ServerEndpoint implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ServerEndpoint.class);

    @Nullable
    private final EventLoop eg;

    @NotNull
    private final AtomicBoolean isClosed = new AtomicBoolean();
    private final String clusterName;

    @Nullable
    private AcceptorEventHandler eah;

    public ServerEndpoint(@NotNull String hostPortDescription,
                          @NotNull AssetTree assetTree,
                          @NotNull NetworkStatsListener networkStatsListener,
                          @NotNull  String clusterName) throws IOException {

        eg = assetTree.root().acquireView(EventLoop.class);
        Threads.<Void, IOException>withThreadGroup(assetTree.root().getView(ThreadGroup.class), () -> {
            start(hostPortDescription, assetTree, networkStatsListener);
            return null;
        });

        assetTree.root().addView(ServerEndpoint.class, this);
        this.clusterName = clusterName;
    }

    public ServerEndpoint(@NotNull String hostPortDescription,
                          @NotNull AssetTree assetTree,
                          String clusterName) throws IOException {

        this(hostPortDescription, assetTree, new NetworkStatsListener() {

            private String host;
            private long port;

            @Override
            public void close() {
                LOG.info(" close() host=" + host + ", port=" + port + ", isConnected=false");
            }

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

            @Override
            public void onRoundTripLatency(long nanosecondLatency) {

            }
        }, clusterName);

    }


    @NotNull
    private AcceptorEventHandler start(@NotNull String hostPortDescription,
                                       @NotNull final AssetTree assetTree,
                                       @NotNull NetworkStatsListener networkStatsListener)
            throws IOException {
        assert eg != null;

        eg.start();
        if (LOG.isInfoEnabled())
            LOG.info("starting server=" + hostPortDescription);

        @Nullable final EventLoop eventLoop = assetTree.root().findOrCreateView(EventLoop.class);
        assert eventLoop != null;

        @NotNull Function<NetworkContext, TcpEventHandler> networkContextTcpEventHandlerFunction =
                BootstrapHandlerFactory.forServerEndpoint()::bootstrapHandlerFactory;
        @NotNull final AcceptorEventHandler eah = new AcceptorEventHandler(
                hostPortDescription,
                networkContextTcpEventHandlerFunction,
                () -> createNetworkContext(assetTree, networkStatsListener));

        eg.addHandler(eah);
        this.eah = eah;
        return eah;
    }

    @NotNull
    private EngineWireNetworkContext createNetworkContext(@NotNull AssetTree assetTree,
                                                          @NotNull final NetworkStatsListener networkStatsListener) {
        @NotNull final EngineWireNetworkContext nc = new EngineWireNetworkContext(assetTree.root(), clusterName);
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
