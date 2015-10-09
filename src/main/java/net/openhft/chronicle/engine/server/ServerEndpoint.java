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

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.map.ChronicleMapKeyValueStore;
import net.openhft.chronicle.engine.server.internal.EngineWireHandler;
import net.openhft.chronicle.network.AcceptorEventHandler;
import net.openhft.chronicle.network.VanillaSessionDetails;
import net.openhft.chronicle.threads.Threads;
import net.openhft.chronicle.threads.api.EventLoop;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

/**
 * Created by Rob Austin
 */
public class ServerEndpoint implements Closeable {

    public static final int HEARTBEAT_INTERVAL_TICKS = Integer.getInteger("heartbeat.interval.ticks", 1_000);
    public static final int HEARTBEAT_TIME_OUT_TICKS = Integer.getInteger("heartbeat.timeout.ticks", 100_000);
    private static final Logger LOGGER = LoggerFactory.getLogger(ChronicleMapKeyValueStore.class);
    @Nullable
    private final EventLoop eg;
    @NotNull
    private final AtomicBoolean isClosed = new AtomicBoolean();
    private final int heartbeatIntervalTicks;
    private final int heartbeatIntervalTimeout;
    @Nullable
    private AcceptorEventHandler eah;

    public ServerEndpoint(String hostPortDescription, @NotNull AssetTree assetTree, @NotNull WireType wire) {
        this(hostPortDescription, assetTree, wire, HEARTBEAT_INTERVAL_TICKS, HEARTBEAT_TIME_OUT_TICKS);
    }

    public ServerEndpoint(@NotNull String hostPortDescription,
                          @NotNull AssetTree assetTree,
                          @NotNull WireType wire,
                          int heartbeatIntervalTicks,
                          int heartbeatIntervalTimeout) {
        this.heartbeatIntervalTicks = heartbeatIntervalTicks;
        this.heartbeatIntervalTimeout = heartbeatIntervalTimeout;
        eg = assetTree.root().acquireView(EventLoop.class);
        Threads.withThreadGroup(assetTree.root().getView(ThreadGroup.class), () -> {
            start(hostPortDescription, assetTree, wire);
            return null;
        });
    }

    @Nullable
    private AcceptorEventHandler start(@NotNull String hostPortDescription,
                                       @NotNull final AssetTree asset,
                                       @NotNull WireType wireType) throws IOException {
        assert eg != null;

        eg.start();
        if (LOGGER.isInfoEnabled())
            LOGGER.info("starting server=" + hostPortDescription);

        final EventLoop eventLoop = asset.root().findOrCreateView(EventLoop.class);
        assert eventLoop != null;

        final AcceptorEventHandler eah = new AcceptorEventHandler(hostPortDescription,
                () -> new EngineWireHandler(wireType, asset),
                VanillaSessionDetails::new,
                heartbeatIntervalTicks,
                heartbeatIntervalTimeout);

        eg.addHandler(eah);
        this.eah = eah;
        return eah;
    }

    private void stop() {
        if (eg != null)
            eg.stop();
    }

    @Override
    public void close() {
        isClosed.set(true);
        stop();

        closeQuietly(eah);
        eah = null;

    }
}
