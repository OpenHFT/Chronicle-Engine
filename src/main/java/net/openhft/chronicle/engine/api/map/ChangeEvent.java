package net.openhft.chronicle.engine.api.map;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by peter on 22/05/15.
 */
public interface ChangeEvent<K, V> extends KeyValueStore.Entry<K, V> {
    V oldValue();

    void apply(MapEventListener<K, V> listener);

    <K2, V2> ChangeEvent<K2, V2> translate(Function<K, K2> keyFunction, Function<V, V2> valueFunction);

    <K2, V2> ChangeEvent<K2, V2> translate(BiFunction<K, K2, K2> keyFunction, BiFunction<V, V2, V2> valueFunction);

    <K2> ChangeEvent<K2, K> pushKey(K2 name);
}
