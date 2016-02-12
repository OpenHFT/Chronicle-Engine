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

package net.openhft.chronicle.engine.fs;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.api.session.SessionDetails;
import net.openhft.chronicle.network.api.session.SessionProvider;
import net.openhft.chronicle.network.connection.ClientConnectionMonitor;
import net.openhft.chronicle.network.connection.SocketAddressSupplier;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static net.openhft.chronicle.network.VanillaSessionDetails.of;

public class HostDetails implements Marshallable, Closeable {
    private final Map<InetSocketAddress, TcpChannelHub> tcpChannelHubs = new ConcurrentHashMap<>();
    public int hostId;
    public int tcpBufferSize;
    public String connectUri;
    public int timeoutMs;

    public HostDetails() {
    }

    public HostDetails(int hostId, int tcpBufferSize, String connectUri, int timeoutMs) {
        this.hostId = hostId;
        this.tcpBufferSize = tcpBufferSize;
        this.connectUri = connectUri;
        this.timeoutMs = timeoutMs;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        wire.read(() -> "hostId").int32(this, (o, i) -> o.hostId = i)
                .read(() -> "tcpBufferSize").int32(this, (o, i) -> o.tcpBufferSize = i)
                .read(() -> "connectUri").text(this, (o, i) -> o.connectUri = i)
                .read(() -> "timeoutMs").int32(this, (o, i) -> o.timeoutMs = i);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(() -> "hostId").int32(hostId)
                .write(() -> "tcpBufferSize").int32(tcpBufferSize)
                .write(() -> "connectUri").text(connectUri)
                .write(() -> "timeoutMs").int32(timeoutMs);
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
        final SessionDetails sessionDetails = asset.root().findView(SessionDetails.class);

        final InetSocketAddress addr = TCPRegistry.lookup(connectUri);

        return tcpChannelHubs.computeIfAbsent(addr, hostPort -> {
            String[] connectURIs = new String[]{connectUri};
            final SocketAddressSupplier socketAddressSupplier = new SocketAddressSupplier(connectURIs, "hostId=" + hostId + ",connectUri=" + connectUri);
            final ClientConnectionMonitor clientConnectionMonitor = asset.findView(ClientConnectionMonitor.class);
            return new TcpChannelHub(new SimpleSessionProvider(sessionDetails), eventLoop, wire, "hostId=" + hostId + ",connectUri=" + connectUri,
                    socketAddressSupplier, true, clientConnectionMonitor, HandlerPriority.TIMER);
        });
    }

    /**
     * @return the {@code TcpChannelHub} if it exists, otherwise {@code null}
     */
    public TcpChannelHub tcpChannelHub() {
        return tcpChannelHubs.get(TCPRegistry.lookup(connectUri));
    }

    @Override
    public void close() {
        tcpChannelHubs.values().forEach(Closeable::closeQuietly);
    }

    @NotNull
    @Override
    public String toString() {
        return "HostDetails{" +
                "hostId=" + hostId +
                ", connectUri='" + connectUri +
                '}';
    }

    /**
     * implements SessionProvider but always returns the same session details regardless of thread
     */
    private class SimpleSessionProvider implements SessionProvider {
        private final SessionDetails sessionDetails;

        public SimpleSessionProvider(@Nullable SessionDetails sessionDetails) {
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
