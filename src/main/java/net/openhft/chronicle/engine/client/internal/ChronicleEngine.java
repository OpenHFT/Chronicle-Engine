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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.ChronicleContext;
import net.openhft.chronicle.engine.FilePerKeyMapSubscription;
import net.openhft.chronicle.engine.MapEventListener;
import net.openhft.chronicle.engine.Subscription;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.FilePerKeyMap;
import net.openhft.chronicle.set.ChronicleSet;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

import static java.util.Collections.synchronizedMap;
import static net.openhft.chronicle.map.ChronicleMapBuilder.of;

/**
 * Created by peter.lawrey on 09/10/14.
 */
public class ChronicleEngine implements ChronicleContext, Closeable {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ChronicleEngine.class);

    //   private final Map<String, ChronicleQueue> queues = Collections.synchronizedMap(new
    //     LinkedHashMap<>());
    private final Map<String, Map<byte[], byte[]>> underlyingMaps
            = synchronizedMap(new LinkedHashMap<>());
    private final Map<String, ChronicleMap> maps = synchronizedMap(new LinkedHashMap<>());

    private final Map<String, ChronicleMap<CharSequence, CharSequence>> chronCharSequenceCharSequenceMap = synchronizedMap(new LinkedHashMap<>());
    private final Map<String, ChronicleMap<Integer, Integer>> chronIntegerIntegerMap = synchronizedMap
            (new LinkedHashMap<>());
    private final Map<String, ChronicleMap<Integer, CharSequence>> chronIntegerStringMap =
            synchronizedMap(new LinkedHashMap<>());
    private final Map<String, FilePerKeyMap> fpMaps = synchronizedMap(new LinkedHashMap<>());
    private final Map<String, ChronicleSet> sets = synchronizedMap(new LinkedHashMap<>());

    public ChronicleEngine() {
        // todo config port and identifiers
        final byte localIdentifier = (byte) 1;
        final int serverPort = 8085;

    }
   /*
    public void setQueue(String fullName, ChronicleQueue chronicle) {
        queues.put(fullName, chronicle);
    }

    @Override
    public ChronicleQueue getQueue(String fullName) {
        return queues.get(fullName);
    }
    */

    public void setMap(String name, ChronicleMap map) throws IOException {
        maps.put(name, map);
    }

    @Override
    public <K, V> ChronicleMap<K, V> getMap(String name, final Class<K> kClass, final Class<V> vClass)
            throws IOException {

        // TODO make this configurable.
        long entries = 1000;
        final int maxValueSize = 4 << 10;
        final int maxKeySize = 64;
        final boolean putReturnsNull = true;
        final boolean removeReturnsNull = true;

        // if its a string map the we will use the string map directly
        if (CharSequence.class.isAssignableFrom(kClass) &&
                CharSequence.class.isAssignableFrom(vClass)) {

            if (String.class.isAssignableFrom(kClass) &&
                    String.class.isAssignableFrom(vClass))
                throw new UnsupportedOperationException("Please use a Map<CharSequence,CharSequence> rather than a Map<String,String>");

            final ChronicleMap<CharSequence, CharSequence> stringMap = chronCharSequenceCharSequenceMap.computeIfAbsent(name,
                    s -> of(CharSequence.class, CharSequence.class)
                            .entries(entries)
                            .averageValueSize(maxValueSize)
                            .averageKeySize(maxKeySize)
                            .putReturnsNull(putReturnsNull)
                            .removeReturnsNull(removeReturnsNull).create());

            return (ChronicleMap) stringMap;
        }

        // if its a string map the we will use the string map directly
        if (Integer.class.isAssignableFrom(kClass) &&
                Integer.class.isAssignableFrom(vClass)) {

            final ChronicleMap<Integer, Integer> stringMap = chronIntegerIntegerMap.computeIfAbsent(name,
                    s -> of(Integer.class, Integer.class)
                            .entries(entries)
                            .putReturnsNull(putReturnsNull)
                            .removeReturnsNull(removeReturnsNull).create());

            return (ChronicleMap) stringMap;
        }

        // if its a string map the we will use the string map directly
        if (Integer.class.isAssignableFrom(kClass) &&
                CharSequence.class.isAssignableFrom(vClass)) {

            if (String.class.isAssignableFrom(kClass) &&
                    String.class.isAssignableFrom(vClass))
                throw new UnsupportedOperationException("Please use a Map<CharSequence,CharSequence> rather than a Map<String,String>");

            final ChronicleMap<Integer, CharSequence> stringMap = chronIntegerStringMap.computeIfAbsent(name,
                    s -> of(Integer.class, CharSequence.class)
                            .entries(entries)
                            .averageValueSize(maxValueSize)
                            .putReturnsNull(putReturnsNull)
                            .removeReturnsNull(removeReturnsNull).create());

            return (ChronicleMap) stringMap;
        }

        throw new UnsupportedOperationException("todo");
    }

    @Override
    public FilePerKeyMap getFilePerKeyMap(String name) {
        return fpMaps.computeIfAbsent(name,
                k -> {
                    try {
                        return new FilePerKeyMap(k);
                    } catch (IOException e) {
                        Jvm.rethrow(e);
                        return null;
                    }
                });
    }

    @Override
    public <I> I getService(Class<I> iClass, String name, Class... args) throws IOException {
        //  if (iClass == Chronicle.class)
        //    return (I) getQueue(fullName);
        if (iClass == ChronicleSet.class)
            return (I) getSet(name, args[0]);
        if (iClass == ChronicleMap.class)
            return (I) getMap(name, args[0], args[1]);
        throw new UnsupportedOperationException();
    }

    @Override
    public <K, V> Subscription<K, MapEventListener<K, V>> createMapSubscription(String name, Class<K> kClass, Class<V> vClass) {
        return createSubscription(name, kClass, (Class<MapEventListener<K, V>>) (Class) MapEventListener.class);
    }

    public <K, V> Subscription<K, MapEventListener<K, V>> createFilePerKeyMapMapSubscription(String name) {
        //Find the corresponding map
        FilePerKeyMap filePerKeyMap = fpMaps.get(name);
        return new FilePerKeyMapSubscription(filePerKeyMap);
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

    @Override
    public Logger getLogger(String name) {
        return Logger.getLogger(name);
    }

    @Override
    public void close() throws IOException {
        fpMaps.values().forEach(FilePerKeyMap::close);
        maps.values().forEach(ChronicleMap::close);

        for (Map<String, ChronicleMap> map : new Map[]{chronIntegerStringMap, chronIntegerStringMap}) {
            map.values().forEach(ChronicleMap::close);
        }
    }

    @Override
    public <K, V> ChronicleMap<K, V> getChronicleMap(String name, Class<K> kClass, Class<V> vClass) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <K, C> Subscription<K, C> createSubscription(String name, Class<K> kClass, Class<C> callbackClass) {
        throw new UnsupportedOperationException();
    }
}
