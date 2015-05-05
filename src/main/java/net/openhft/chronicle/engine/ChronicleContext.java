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

package net.openhft.chronicle.engine;

import net.openhft.chronicle.engine.old.ChronicleCluster;
import net.openhft.chronicle.engine.old.ChronicleThreadPool;
import net.openhft.chronicle.map.ChronicleMap;

import net.openhft.chronicle.set.ChronicleSet;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Created by peter.lawrey on 09/10/14.
 */
public interface ChronicleContext {
    //ChronicleQueue getQueue(String name);

    // get any map
    <K, V> ChronicleMap<K, V> getMap(String name, Class<K> kClass, Class<V> vClass) throws IOException;

    // get a subscription on a map
    <K, V> Subscription<K, MapEventListener<K, V>> createMapSubscription(String name, Class<K> kClass, Class<V> vClass);

    <E> ChronicleSet<E> getSet(String name, Class<E> eClass);

    // A generic service
    <I> I getService(Class<I> iClass, String name, Class... args) throws IOException;

    // A generic subscription
    <K, C> Subscription<K, C> getSubscription(String name, Class<K> kClass, Class<C> callbackClass);

    ChronicleThreadPool getThreadPool(String name);

    ChronicleCluster getCluster(String name); // check name of cluster interface.

    Logger getLogger(String name);
}
