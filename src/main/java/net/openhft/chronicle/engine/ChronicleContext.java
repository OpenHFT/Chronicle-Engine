package net.openhft.chronicle.engine;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.set.ChronicleSet;

import java.util.logging.Logger;

/**
 * Created by peter on 09/10/14.
 */
public interface ChronicleContext {
    Chronicle getQueue(String name);

    <K, V> ChronicleMap<K, V> getMap(String name, Class<K> kClass, Class<V> vClass);

    <E> ChronicleSet<E> getSet(String name, Class<E> eClass);

    ChronicleThreadPool getThreadPool(String name);

    ChronicleCluster getCluster(String name); // check name of cluster interface.

    Logger getLogger(String name);
}
