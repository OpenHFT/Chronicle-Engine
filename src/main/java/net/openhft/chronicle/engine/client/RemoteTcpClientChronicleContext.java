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
import net.openhft.chronicle.engine.ChronicleContext;
import net.openhft.chronicle.engine.MapEventListener;
import net.openhft.chronicle.engine.Subscription;
import net.openhft.chronicle.engine.client.internal.RemoteClientServiceLocator;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.chronicle.wire.Wire;
import net.openhft.lang.model.constraints.NotNull;

import java.io.IOException;
import java.util.function.Function;
import java.util.logging.Logger;

/**
 * used to connect to remove engines over TCP/IP Created by Rob Austin
 */
public class RemoteTcpClientChronicleContext implements ChronicleContext, Closeable {

    RemoteClientServiceLocator remoteClientServiceLocator;

    public RemoteTcpClientChronicleContext(@NotNull final String hostname,
                                           int port,
                                           byte identifier, Function<Bytes, Wire> wireType)
            throws IOException {
        this.remoteClientServiceLocator = new RemoteClientServiceLocator(
                hostname,
                port,
                identifier,
                wireType);
    }

  /*   @Override
    public ChronicleQueue getQueue(String fullName) {
        return remoteClientServiceLocator.getService(ChronicleQueue.class, fullName);
    }*/

    @Override
    public <K, V> ChronicleMap<K, V> getMap(String name, Class<K> kClass, Class<V> vClass) {
        return remoteClientServiceLocator.getService(ChronicleMap.class, name, kClass, vClass);
    }

    @Override
    public <K, V> ChronicleMap<K, V> getChronicleMap(String name, Class<K> kClass, Class<V> vClass) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <E> ChronicleSet<E> getSet(String name, Class<E> eClass) {
        return null;
    }

    @Override
    public <I> I getService(Class<I> iClass, String name, Class... args) {
        return remoteClientServiceLocator.getService(iClass, name, args);
    }

    @Override
    public <K, V> Subscription<K, MapEventListener<K, V>> createMapSubscription(String name, Class<K> kClass, Class<V> vClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <K, C> Subscription<K, C> createSubscription(String name, Class<K> kClass, Class<C> callbackClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Logger getLogger(String name) {
        throw new UnsupportedOperationException("todo (getLogger)");
    }

    @Override
    public void close() {
        remoteClientServiceLocator.close();
    }
}
