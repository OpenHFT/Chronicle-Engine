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

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.engine.ChronicleContext;
import net.openhft.chronicle.engine.old.ChronicleCluster;
import net.openhft.chronicle.engine.old.ChronicleThreadPool;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.EngineMap;
import net.openhft.chronicle.map.MapWireConnectionHub;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.lang.Jvm;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by peter.lawrey on 09/10/14.
 */
public class ChronicleEngine implements ChronicleContext, Closeable {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ChronicleEngine.class);

    private final Map<String, ChronicleQueue> queues = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Map<String, ChronicleSet> sets = Collections.synchronizedMap(new
            LinkedHashMap<>());
    private final Map<String, ChronicleMap> maps = Collections.synchronizedMap(new
            LinkedHashMap<>());
    private final Map<String, ChronicleThreadPool> threadPools = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Map<String, ChronicleCluster> clusters = Collections.synchronizedMap(new LinkedHashMap<>());
    private MapWireConnectionHub mapWireConnectionHub = null;

    public ChronicleEngine() {

        // todo config port and identifiers
        final byte localIdentifier = (byte) 1;
        final int serverPort = 8085;

        try {
            mapWireConnectionHub = new MapWireConnectionHub(localIdentifier, serverPort);
        } catch (IOException e) {
            LOG.error("", e);
        }

    }

    public void setQueue(String name, ChronicleQueue chronicle) {
        queues.put(name, chronicle);
    }

    @Override
    public ChronicleQueue getQueue(String name) {
        return queues.get(name);
    }

    public void setMap(String name, ChronicleMap map) throws IOException {

        final Map o = getMap(name, map.keyClass(), map.valueClass());
        o.clear();
        o.putAll(map);
    }

    @Override
    public <K, V> ChronicleMap<K, V> getMap(String name, Class<K> kClass, Class<V> vClass) throws
            IOException {


        return maps.computeIfAbsent(name, n ->
        {
            try {

                // todo set this from config
                final int entries = 100;

                ChronicleMap map = new EngineMap<>(
                        n,
                        kClass,
                        vClass,
                        mapWireConnectionHub,
                        TextWire.class,
                        entries);

                validateClasses(map, kClass, vClass);

                return map;

            } catch (IOException e) {
                Jvm.rethrow(e);
                return null;
            }


        });

    }

    @Override
    public <I> I getService(Class<I> iClass, String name, Class... args) throws IOException {
        if (iClass == Chronicle.class)
            return (I) getQueue(name);
        if (iClass == ChronicleSet.class)
            return (I) getSet(name, args[0]);
        if (iClass == ChronicleMap.class)
            return (I) getMap(name, args[0], args[1]);
        throw new UnsupportedOperationException();
    }

    private <K, V> void validateClasses(Map map, Class<K> kClass, Class<V> vClass) {
        // TODO runtime check the key and values classes match
    }

    public void setSet(String name, ChronicleSet set) {
        sets.put(name, set);
    }

    @Override
    public <E> ChronicleSet<E> getSet(String name, Class<E> eClass) {
        ChronicleSet set = sets.get(name);
        if (set != null)
            validateClasses(set, eClass);
        return set;
    }

    private <E> void validateClasses(ChronicleSet set, Class<E> eClass) {
        // TODO runtime check the element class matches.
    }

    public void setThreadPool(String name, ChronicleThreadPool threadPool) {
        threadPools.put(name, threadPool);
    }

    @Override
    public ChronicleThreadPool getThreadPool(String name) {
        return threadPools.get(name);
    }

    public void setCluster(String name, ChronicleCluster cluster) {
        clusters.put(name, cluster);
    }

    @Override
    public ChronicleCluster getCluster(String name) {
        return clusters.get(name);
    }

    @Override
    public Logger getLogger(String name) {
        return Logger.getLogger(name);
    }

    @Override
    public void close() throws IOException {
       mapWireConnectionHub.close();
    }
}
