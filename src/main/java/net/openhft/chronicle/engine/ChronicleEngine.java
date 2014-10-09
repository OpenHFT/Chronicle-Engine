package net.openhft.chronicle.engine;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleSet;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by peter on 09/10/14.
 */
public class ChronicleEngine implements ChronicleContext {
    private final Map<String, Chronicle> queues = Collections.synchronizedMap(new LinkedHashMap<String, Chronicle>());
    private final Map<String, ChronicleMap> maps = Collections.synchronizedMap(new LinkedHashMap<String, ChronicleMap>());
    private final Map<String, ChronicleSet> sets = Collections.synchronizedMap(new LinkedHashMap<String, ChronicleSet>());
    private final Map<String, ChronicleThreadPool> threadPools = Collections.synchronizedMap(new LinkedHashMap<String, ChronicleThreadPool>());
    private final Map<String, ChronicleCluster> clusters = Collections.synchronizedMap(new LinkedHashMap<String, ChronicleCluster>());

    public void setQueue(String name, Chronicle chronicle) {
        queues.put(name, chronicle);
    }

    @Override
    public Chronicle getQueue(String name) {
        return queues.get(name);
    }

    public void setMap(String name, ChronicleMap map) {
        maps.put(name, map);
    }

    @Override
    public <K, V> ChronicleMap<K, V> getMap(String name, Class<K> kClass, Class<V> vClass) {
        ChronicleMap map = maps.get(name);
        if (map != null)
            validateClasses(map, kClass, vClass);
        return map;
    }

    private <K, V> void validateClasses(ChronicleMap map, Class<K> kClass, Class<V> vClass) {

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
}
