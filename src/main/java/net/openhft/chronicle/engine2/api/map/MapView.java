package net.openhft.chronicle.engine2.api.map;

import net.openhft.chronicle.engine2.api.Assetted;
import net.openhft.chronicle.engine2.api.View;

import java.util.concurrent.ConcurrentMap;

/**
 * Created by peter on 22/05/15.
 */
public interface MapView<K, MV, V> extends ConcurrentMap<K, V>, Assetted<KeyValueStore<K, MV, V>>, View {
    default boolean keyedView() {
        return true;
    }
}
