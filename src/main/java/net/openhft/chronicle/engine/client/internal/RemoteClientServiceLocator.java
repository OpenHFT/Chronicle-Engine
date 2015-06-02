/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.engine.client.internal;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.core.MemoryUnit;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ClientWiredChronicleMapStatelessBuilder;
import net.openhft.chronicle.network.connection.ClientWiredStatelessTcpConnectionHub;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Created by Rob Austin
 */
public class RemoteClientServiceLocator {

    private final ClientWiredStatelessTcpConnectionHub hub;

    public RemoteClientServiceLocator(@NotNull String hostname,
                                      int port,
                                      byte identifier,
                                      @NotNull Function<Bytes, Wire> byteToWire) throws IOException {

        final InetSocketAddress inetSocketAddress = new InetSocketAddress(hostname, port);
        int tcpBufferSize = (int) MemoryUnit.MEGABYTES.toBytes(2) + 1024;
        long timeoutMs = TimeUnit.SECONDS.toMillis(20);

        hub = new ClientWiredStatelessTcpConnectionHub(identifier,
                false,
                inetSocketAddress,
                tcpBufferSize,
                timeoutMs, byteToWire);
    }

    public <I> I getService(Class<I> iClass, String name, Class... args) {
        try {

            if (ChronicleMap.class.isAssignableFrom(iClass)) {
                final Class kClass = args[0];
                final Class vClass = args[1];
                return (I) mapInstance(kClass, vClass, name);

            }

        } catch (IOException e) {
            throw new IORuntimeException(e);
        }

        throw new IllegalStateException("iClass=" + iClass + " not supported");
    }
/*
    private <I> I newQueueInstance(String fullName) {
        return (I) new ClientWiredChronicleQueueStateless(hub, fullName);
    }*/

    private <I, KI, VI> I mapInstance(Class<KI> kClass, Class<VI> vClass, String name)
            throws IOException {

        return (I) new ClientWiredChronicleMapStatelessBuilder<KI, VI>(hub, kClass, vClass, name)
                .putReturnsNull(true)
                .removeReturnsNull(true)
                .create();
    }

    public void close() {
        hub.close();
    }
}
