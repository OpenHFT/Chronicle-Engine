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

package net.openhft.chronicle.engine.fs;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.api.session.SessionDetails;
import net.openhft.chronicle.network.api.session.SessionProvider;
import net.openhft.chronicle.network.cluster.HostDetails;
import net.openhft.chronicle.network.connection.ClientConnectionMonitor;
import net.openhft.chronicle.network.connection.SocketAddressSupplier;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static net.openhft.chronicle.network.VanillaSessionDetails.of;

public class EngineHostDetails extends HostDetails implements Marshallable, Closeable {

    private final Map<InetSocketAddress, TcpChannelHub> tcpChannelHubs = new ConcurrentHashMap<>();

    EngineHostDetails() {
        super();
    }

    public EngineHostDetails(int hostId, int tcpBufferSize, String connectUri) {
        super();
        hostId(hostId);
        this.tcpBufferSize(tcpBufferSize);
        this.connectUri(connectUri);
    }

    /**
     * @param asset     a point in the asset tree, used to fine the ClientConnectionMonitor
     * @param eventLoop used to process events
     * @param wire      converts from bytes to wire for the type of the wire used
     * @return a new or existing instance of the TcpChannelHub
     */
    public TcpChannelHub acquireTcpChannelHub(@NotNull final Asset asset,
                                              @NotNull final EventLoop eventLoop,
                                              @NotNull final WireType wire) {
        @Nullable
        final SessionDetails sessionDetails = asset.findView(SessionDetails.class);
        String connectUri = connectUri();
        final InetSocketAddress addr = TCPRegistry.lookup(connectUri);
        int hostId = hostId();

        return tcpChannelHubs.computeIfAbsent(addr, hostPort -> {
            String[] connectURIs = new String[]{connectUri};

            final SocketAddressSupplier socketAddressSupplier = new SocketAddressSupplier
                    (connectURIs, "hostId=" + hostId() + ",connectUri=" + connectUri);
            final ClientConnectionMonitor clientConnectionMonitor = asset.findView(ClientConnectionMonitor.class);
            return new TcpChannelHub(new SimpleSessionProvider(sessionDetails), eventLoop, wire, "hostId=" + hostId + ",connectUri=" + connectUri,
                    socketAddressSupplier, true, clientConnectionMonitor, HandlerPriority.TIMER);
        });
    }

    @Override
    public void close() {
        tcpChannelHubs.values().forEach(Closeable::closeQuietly);
    }

    /**
     * @return the {@code TcpChannelHub} if it exists, otherwise {@code null}
     */
    public TcpChannelHub tcpChannelHub() {
        return tcpChannelHubs.get(TCPRegistry.lookup(connectUri()));
    }

    /**
     * implements SessionProvider but always returns the same session details regardless of thread
     */
    private class SimpleSessionProvider implements SessionProvider {
        private final SessionDetails sessionDetails;

        SimpleSessionProvider(@Nullable SessionDetails sessionDetails) {
            this.sessionDetails = (sessionDetails == null) ? of("", "", "") : sessionDetails;
        }

        /**
         * @return the current session details
         */
        @Nullable
        public SessionDetails get() {
            return sessionDetails;
        }

        /**
         * Replace the session details
         *
         * @param sessionDetails to set to
         */
        public void set(@NotNull SessionDetails sessionDetails) {
            throw new UnsupportedOperationException();
        }

        /**
         * There is no longer any valid session detaisl and get() will return null.
         */
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
