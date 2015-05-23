package net.openhft.chronicle.engine2.api.map;

import net.openhft.chronicle.engine2.api.Interceptor;

/**
 * Created by peter on 22/05/15.
 */
public interface KeyValueStoreFactory<K, V> extends Interceptor {
    KeyValueStore<K, V> create(String name, Class<K> kClass, Class<V> vClass);
}
