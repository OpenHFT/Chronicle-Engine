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

import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.internal.EngineWireHandler;
import net.openhft.chronicle.network.AcceptorEventHandler;
import net.openhft.chronicle.network.VanillaSessionDetails;
import net.openhft.chronicle.threads.EventGroup;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Rob Austin
 */
public class ServerEndpoint implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ServerEndpoint.class);

    @NotNull
    private EventGroup eg;

    @Nullable
    private AcceptorEventHandler eah;

    public ServerEndpoint(AssetTree assetTree) throws
            IOException {
        this(0, true, assetTree);
    }

    public ServerEndpoint(int port, boolean daemon, AssetTree assetTree) throws IOException {
        eg = new EventGroup(daemon);
        start(port, assetTree);
    }

    @Nullable
    public AcceptorEventHandler start(int port, @NotNull final AssetTree asset) throws IOException {
        eg.start();

        AcceptorEventHandler eah = new AcceptorEventHandler(port, () -> {

            final Map<Long, String> cidToCsp = new HashMap<>();

            try {
                return new EngineWireHandler(cidToCsp, WireType.wire, asset);
            } catch (IOException e) {
                LOG.error("", e);
            }
            return null;
        }, VanillaSessionDetails::new);

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
    public void close() throws IOException {
        stop();
        eg.close();
        eah.close();

    }
}
