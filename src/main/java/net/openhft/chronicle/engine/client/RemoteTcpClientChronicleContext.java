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

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.engine.ChronicleContext;
import net.openhft.chronicle.engine.client.internal.RemoteClientServiceLocator;
import net.openhft.chronicle.engine.old.ChronicleCluster;
import net.openhft.chronicle.engine.old.ChronicleThreadPool;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.set.ChronicleSet;

import java.util.logging.Logger;

/**
 * used to connect to remove engines over TCP/IP Created by Rob Austin
 */
public class RemoteTcpClientChronicleContext implements ChronicleContext {

    RemoteClientServiceLocator remoteClientServiceLocator;

    public RemoteTcpClientChronicleContext(RemoteClientServiceLocator remoteClientServiceLocator) {
        this.remoteClientServiceLocator = remoteClientServiceLocator;
    }

    @Override
    public Chronicle getQueue(String name) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <K, V> ChronicleMap<K, V> getMap(String name, Class<K> kClass, Class<V> vClass) {
        return remoteClientServiceLocator.getService(ChronicleMap.class, name, kClass, vClass);
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
    public ChronicleThreadPool getThreadPool(String name) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public ChronicleCluster getCluster(String name) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Logger getLogger(String name) {
        throw new UnsupportedOperationException("todo");
    }


}
