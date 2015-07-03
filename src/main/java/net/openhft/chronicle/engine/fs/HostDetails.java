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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.engine.api.session.SessionProvider;
import net.openhft.chronicle.engine.session.VanillaSessionProvider;
import net.openhft.chronicle.network.VanillaSessionDetails;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.threads.api.EventLoop;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Created by peter.lawrey on 17/06/2015.
 */
public class HostDetails implements Marshallable {
    public int hostId;
    public int tcpBufferSize;
    public String hostname;
    public int port;
    public int timeoutMs;


    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        wire.read(() -> "hostId").int32(i -> hostId = i)
                .read(() -> "tcpBufferSize").int32(i -> tcpBufferSize = i)
                .read(() -> "hostname").text(i -> hostname = i)
                .read(() -> "port").int32(i -> port = i)
                .read(() -> "timeoutMs").int32(i -> timeoutMs = i);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(() -> "hostId").int32(hostId)
                .write(() -> "tcpBufferSize").int32(tcpBufferSize)
                .write(() -> "hostname").text(hostname)
                .write(() -> "port").int32(port)
                .write(() -> "timeoutMs").int32(timeoutMs);
    }

    public TcpChannelHub acquireTcpChannelHub(EventLoop eventLoop, Function<Bytes, Wire> wire) {
        final HostPort key = new HostPort(hostname, port);

        return tcpChannelHubs.computeIfAbsent(key, hostPort ->
                new TcpChannelHub(sessionProvider(), hostPort.host, hostPort.port, eventLoop, wire));
    }

    @NotNull
    private SessionProvider sessionProvider() {
        SessionProvider sessionProvider = new VanillaSessionProvider();
        VanillaSessionDetails sessionDetails = new VanillaSessionDetails();
        sessionDetails.setUserId(System.getProperty("user.name"));
        sessionProvider.set(sessionDetails);
        return sessionProvider;
    }

    @NotNull
    private static Map<HostPort, TcpChannelHub> tcpChannelHubs = new ConcurrentHashMap<>();

    class HostPort {
        String host;
        int port;

        HostPort(@NotNull final String host, final int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public boolean equals(@Nullable final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final HostPort hostPort = (HostPort) o;

            if (port != hostPort.port) return false;
            return host.equals(hostPort.host);

        }

        @Override
        public int hashCode() {
            int result = host.hashCode();
            result = 31 * result + port;
            return result;
        }
    }
}
