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

package net.openhft.chronicle.engine.client;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.util.Closeable;
import net.openhft.chronicle.engine.client.internal.RemoteClientServiceLocator;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.wire.Wire;
import net.openhft.lang.model.constraints.NotNull;

import java.io.IOException;
import java.util.function.Function;

/**
 * used to connect to remove engines over TCP/IP Created by Rob Austin
 */
public class RemoteChassis implements Closeable {

    RemoteClientServiceLocator remoteClientServiceLocator;

    public RemoteChassis(@NotNull final String hostname,
                         int port,
                         byte identifier, Function<Bytes, Wire> wireType)
            throws IOException {
        this.remoteClientServiceLocator = new RemoteClientServiceLocator(
                hostname,
                port,
                identifier,
                wireType);
    }

    public <K, V> ChronicleMap<K, V> acquireMap(String name, Class<K> kClass, Class<V> vClass) {
        return remoteClientServiceLocator.getService(ChronicleMap.class, name, kClass, vClass);
    }

    @Override
    public void close() {
        remoteClientServiceLocator.close();
    }
}
