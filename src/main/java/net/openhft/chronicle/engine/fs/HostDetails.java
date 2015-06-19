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

import net.openhft.chronicle.engine.map.ReplicationHub;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;

/**
 * Created by peter.lawrey on 17/06/2015.
 */
public class HostDetails implements Marshallable {
    public int hostId;
    int tcpBufferSize;
    String hostname;
    int port;
    int timeoutMs;

    @Override
    public void readMarshallable(WireIn wire) throws IllegalStateException {
        wire.read(() -> "hostId").int32(i -> hostId = i)
                .read(() -> "tcpBufferSize").int32(i -> tcpBufferSize = i)
                .read(() -> "hostname").text(i -> hostname = i)
                .read(() -> "port").int32(i -> port = i)
                .read(() -> "timeoutMs").int32(i -> timeoutMs = i);
    }

    @Override
    public void writeMarshallable(WireOut wire) {
        wire.write(() -> "hostId").int32(hostId)
                .write(() -> "tcpBufferSize").int32(tcpBufferSize)
                .write(() -> "hostname").text(hostname)
                .write(() -> "port").int32(port)
                .write(() -> "timeoutMs").int32(timeoutMs);
    }

    public ReplicationHub acquireReplicationHub() {
//        asset.acquireView
//                (requestContext(context.name() + "&" + address).viewType(ReplicationHub.class));
        throw new UnsupportedOperationException("todo");
    }
}
