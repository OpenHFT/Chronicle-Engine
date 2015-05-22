package net.openhft.chronicle.engine2.api.map;

import net.openhft.chronicle.engine2.api.Asset;
import net.openhft.chronicle.engine2.api.Interceptor;

/**
 * Created by peter on 22/05/15.
 */
public interface EntrySetViewFactory<K, V> extends Interceptor {
    EntrySetView<K, V> create(Asset asset, KeyValueStore<K, V> kvStore, String queryString);
}
