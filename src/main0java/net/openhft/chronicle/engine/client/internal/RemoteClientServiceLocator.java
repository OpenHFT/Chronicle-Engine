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

import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ClientWiredChronicleMapStatelessBuilder;

import net.openhft.lang.MemoryUnit;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Created by Rob Austin
 */
public class RemoteClientServiceLocator {

    private final ClientWiredStatelessTcpConnectionHub hub;

    public RemoteClientServiceLocator(@NotNull String hostname,
                                      int port,
                                      byte identifier) throws IOException {


        final InetSocketAddress inetSocketAddress = new InetSocketAddress(hostname, port);

        int tcpBufferSize = (int) MemoryUnit.KILOBYTES.toBytes(64);

        long timeoutMs = TimeUnit.SECONDS.toMillis(20);

        hub = new ClientWiredStatelessTcpConnectionHub(identifier, false, inetSocketAddress, tcpBufferSize, timeoutMs);

    }


    private <K, V> ChronicleMap<K, V> newMapInstance(@NotNull String name,
                                                     @NotNull Class<K> kClass,
                                                     @NotNull Class<V> vClass) throws IOException {
        return mapInstance(kClass, vClass, name);
    }


    public <I> I getService(Class<I> iClass, String name, Class... args) {

        try {

            if (ChronicleMap.class.isAssignableFrom(iClass)) {
                final Class kClass = args[0];
                final Class vClass = args[1];
                return (I) newMapInstance(name, kClass, vClass);
            }/*    } else if (ChronicleQueue.class.isAssignableFrom(iClass)) {
                return (I) newQueueInstance(name);
            }
*/
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }

        throw new IllegalStateException("iClass=" + iClass + " not supported");
    }
/*
    private <I> I newQueueInstance(String name) {
        return (I) new ClientWiredChronicleQueueStateless(hub, name);
    }*/

    private <I, KI, VI> I mapInstance(Class<KI> kClass, Class<VI> vClass, String name)
            throws IOException {

        return (I) new ClientWiredChronicleMapStatelessBuilder<KI, VI>(
                hub,
                kClass,
                vClass,
                name).create();
    }


    public void close() {
        hub.close();
    }
}
