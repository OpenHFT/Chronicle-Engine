package net.openhft.chronicle.engine.api.map;

import java.util.function.Function;

/**
 * Created by peter on 22/05/15.
 */
public interface MapEvent<K, V> extends KeyValueStore.Entry<K, V> {
    void apply(MapEventListener<K, V> listener);

    <K2, V2> MapEvent<K2, V2> translate(Function<K, K2> keyFunction, Function<V, V2> valueFunction);
}
