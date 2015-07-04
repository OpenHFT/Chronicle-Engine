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
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.threads.api.EventLoop;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Created by peter.lawrey on 17/06/2015.
 */
public class HostDetails implements Marshallable {
    public int hostId;
    public int tcpBufferSize;
    public String connectUri;
    public int timeoutMs;
    private final Map<InetSocketAddress, TcpChannelHub> tcpChannelHubs = new ConcurrentHashMap<>();


    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        wire.read(() -> "hostId").int32(i -> hostId = i)
                .read(() -> "tcpBufferSize").int32(i -> tcpBufferSize = i)
                .read(() -> "connectUri").text(i -> connectUri = i)
                .read(() -> "timeoutMs").int32(i -> timeoutMs = i);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(() -> "hostId").int32(hostId)
                .write(() -> "tcpBufferSize").int32(tcpBufferSize)
                .write(() -> "connectUri").text(connectUri)
                .write(() -> "timeoutMs").int32(timeoutMs);
    }


    public TcpChannelHub acquireTcpChannelHub(Asset asset, EventLoop eventLoop, Function<Bytes, Wire> wire) {
        InetSocketAddress addr = TCPRegistry.lookup(connectUri);
        SessionProvider sessionProvider = asset.findOrCreateView(SessionProvider.class);
        return tcpChannelHubs.computeIfAbsent(addr, hostPort ->
                new TcpChannelHub(sessionProvider, connectUri, eventLoop, wire));
    }
}
