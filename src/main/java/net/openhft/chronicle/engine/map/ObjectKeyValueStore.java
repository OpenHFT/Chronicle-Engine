package net.openhft.chronicle.engine.map;

/**
 * Created by peter on 01/06/15.
 */
public interface ObjectKeyValueStore<K, MV, V> extends AuthenticatedKeyValueStore<K, MV, V> {
    Class<K> keyType();

    Class<V> valueType();
}
