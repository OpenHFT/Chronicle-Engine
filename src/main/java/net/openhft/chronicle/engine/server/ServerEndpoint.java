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

    private static final Logger LOGGER = LoggerFactory.getLogger(ChronicleMapKeyValueStore.class);
    @Nullable
    private final EventLoop eg;
    @Nullable
    private AcceptorEventHandler eah;
    @NotNull
    private final AtomicBoolean isClosed = new AtomicBoolean();

    public ServerEndpoint(String hostPortDescription, @NotNull AssetTree assetTree, @NotNull WireType wire) throws IOException {
        eg = assetTree.root().acquireView(EventLoop.class);
        Threads.withThreadGroup(assetTree.root().getView(ThreadGroup.class), () -> {
            start(hostPortDescription, assetTree, wire);
            return null;
        });
    }

    @Nullable
    private AcceptorEventHandler start(String hostPortDescription, @NotNull final AssetTree asset, @NotNull WireType wireType) throws IOException {
        eg.start();
        if (LOGGER.isDebugEnabled())
            LOGGER.info("starting server=" + hostPortDescription);
        AcceptorEventHandler eah = new AcceptorEventHandler(hostPortDescription,
                () -> new EngineWireHandler(wireType, asset), VanillaSessionDetails::new);

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
