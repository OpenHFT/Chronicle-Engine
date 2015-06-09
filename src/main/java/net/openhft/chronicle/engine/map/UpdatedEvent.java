package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.map.MapEventListener;
import net.openhft.chronicle.engine.api.map.MapReplicationEvent;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Created by peter on 22/05/15.
 */
public class UpdatedEvent<K, V> implements MapReplicationEvent<K, V> {
    private final K key;
    private final V oldValue;
    private final V value;
    private final int identifier;
    private final long timeStampMS;

    private UpdatedEvent(K key, V oldValue, V value, int identifier, long timeStampMS) {
        this.key = key;
        this.oldValue = oldValue;
        this.value = value;
        this.identifier = identifier;
        this.timeStampMS = timeStampMS;
    }

    @NotNull
    public static <K, V> UpdatedEvent<K, V> of(K key, V oldValue, V value, int identifier, long timeStampMS) {
        return new UpdatedEvent<>(key, oldValue, value, identifier, timeStampMS);
    }

    @NotNull
    @Override
    public <K2, V2> MapReplicationEvent<K2, V2> translate(@NotNull Function<K, K2> keyFunction, @NotNull Function<V, V2> valueFunction) {
        return new UpdatedEvent<>(keyFunction.apply(key), valueFunction.apply(oldValue), valueFunction.apply(value), identifier, timeStampMS);
    }

    public K key() {
        return key;
    }

    @Override
    public V oldValue() {
        return oldValue;
    }

    public V value() {
        return value;
    }

    @Override
    public boolean isDeleted() {
        return false;
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
    public void apply(@NotNull MapEventListener<K, V> listener) {
        listener.update(key, oldValue, value);
    }

    @Override
    public int hashCode() {
        return Objects.hash("updated", key, value);
    }

    @Override
    public boolean equals(Object obj) {
        return Optional.ofNullable(obj)
                .filter(o -> o instanceof UpdatedEvent)
                .map(o -> (UpdatedEvent<K, V>) o)
                //.filter(e -> timeStampMS == e.timeStampMS)
                //.filter(e -> identifier == e.identifier)
                .filter(e -> Objects.equals(key, e.key))
                .filter(e -> Objects.equals(oldValue, e.oldValue))
                .filter(e -> Objects.equals(value, e.value))
                .isPresent();
    }

    @NotNull
    @Override
    public String toString() {
        return "UpdatedEvent{" +
                "key=" + key +
                ", oldValue=" + oldValue +
                ", value=" + value +
                '}';
    }
}
