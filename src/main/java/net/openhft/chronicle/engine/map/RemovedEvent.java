package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.map.MapEventListener;
import net.openhft.chronicle.engine.api.map.MapReplicationEvent;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Created by peter on 22/05/15.
 */
public class RemovedEvent<K, V> implements MapReplicationEvent<K, V> {
    private final K key;
    private final V value;
    private final int identifier;
    private final long timeStampMS;

    private RemovedEvent(K key, V value, int identifier, long timeStampMS) {
        this.key = key;
        this.value = value;
        this.identifier = identifier;
        this.timeStampMS = timeStampMS;
    }

    public static <K, V> RemovedEvent<K, V> of(K key, V value, int identifier, long timeStampMS) {
        return new RemovedEvent<>(key, value, identifier, timeStampMS);
    }

    @Override
    public <K2, V2> MapReplicationEvent<K2, V2> translate(Function<K, K2> keyFunction, Function<V, V2> valueFunction) {
        return new RemovedEvent<>(keyFunction.apply(key), valueFunction.apply(value), identifier, timeStampMS);
    }

    public K key() {
        return key;
    }

    @Override
    public V oldValue() {
        return value;
    }

    public V value() {
        return null;
    }

    @Override
    public boolean isDeleted() {
        return true;
    }

    @Override
    public int identifier() {
        return identifier;
    }

    @Override
    public long timeStampMS() {
        return timeStampMS;
    }

    @Override
    public long dataUpToTimeStampMS() {
        return 0L;
    }

    @Override
    public void apply(MapEventListener<K, V> listener) {
        listener.remove(key, value);
    }

    @Override
    public int hashCode() {
        return Objects.hash("removed", key, value);
    }

    @Override
    public boolean equals(Object obj) {
        return Optional.ofNullable(obj)
                .filter(o -> o instanceof RemovedEvent)
                .map(o -> (RemovedEvent<K, V>) o)
                .filter(e -> timeStampMS == e.timeStampMS)
                .filter(e -> identifier == e.identifier)
                .filter(e -> Objects.equals(key, e.key))
                .filter(e -> Objects.equals(value, e.value))
                .isPresent();
    }

    @Override
    public String toString() {
        return "RemovedEvent{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }
}
