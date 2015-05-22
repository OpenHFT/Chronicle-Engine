package net.openhft.chronicle.engine2.api.map;

import net.openhft.chronicle.engine2.api.Asset;
import net.openhft.chronicle.engine2.api.Interceptor;
import net.openhft.chronicle.engine2.map.MapView;

/**
 * Created by peter on 22/05/15.
 */
public interface MapViewFactory<K, V> extends Interceptor {
    MapView<K, V> create(Asset asset, KeyValueStore<K, V> kvStore, String queryString);
}
