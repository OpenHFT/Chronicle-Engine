package net.openhft.chronicle.engine2.api.set;

import net.openhft.chronicle.map.MapEntry;

import java.util.Set;

/**
 * Created by peter on 28/05/15.
 */
public interface EntrySet<K, V> extends Set<MapEntry<K, V>> {
}
