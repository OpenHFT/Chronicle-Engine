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
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.server.internal.EngineWireHandler;
import net.openhft.chronicle.network.AcceptorEventHandler;
import net.openhft.chronicle.network.VanillaSessionDetails;
import net.openhft.chronicle.threads.Threads;
import net.openhft.chronicle.threads.api.EventLoop;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

/**
 * Created by Rob Austin
 */
public class ServerEndpoint implements Closeable {

    @Nullable
    private EventLoop eg;
    @Nullable
    private AcceptorEventHandler eah;
    @NotNull
    private AtomicBoolean isClosed = new AtomicBoolean();

    public ServerEndpoint(@NotNull AssetTree assetTree) throws
            IOException {
        this(0, assetTree);
    }

    public ServerEndpoint(int port, @NotNull AssetTree assetTree) throws IOException {
        eg = assetTree.root().acquireView(EventLoop.class, RequestContext.requestContext());
        Threads.withThreadGroup(assetTree.root().getView(ThreadGroup.class), () -> {
            start(port, assetTree);
            return null;
        });
    }

    @Nullable
    public AcceptorEventHandler start(int port, @NotNull final AssetTree asset) throws IOException {
        eg.start();

        AcceptorEventHandler eah = new AcceptorEventHandler(port, () -> new EngineWireHandler
                (WireType.wire, asset, isClosed), VanillaSessionDetails::new);

        eg.addHandler(eah);
        this.eah = eah;
        return eah;
    }

    public int getPort() throws IOException {
        return eah.getLocalPort();
    }

    public void stop() {
        eg.stop();
    }

    @Override
    public void close() {
        isClosed.set(true);
        stop();
        closeQuietly(eg);
        eg = null;
        closeQuietly(eah);
        eah = null;

    }
}
