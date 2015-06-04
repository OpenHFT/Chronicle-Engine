package net.openhft.chronicle.engine.api.map;

import java.util.function.Function;

/**
 * Created by peter.lawrey on 03/06/2015.
 */
public interface MapReplicationEvent<K, V> extends MapEvent<K, V> {
    @Override
    K key();

    V oldValue();

    @Override
    V value();

    boolean isDeleted();

    int identifier();

    long timeStampMS();

    long dataUpToTimeStampMS();

    <K2, V2> MapReplicationEvent<K2, V2> translate(Function<K, K2> keyFunction, Function<V, V2> valueFunction);
}
