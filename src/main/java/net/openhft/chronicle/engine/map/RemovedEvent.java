package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapEventListener;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by peter on 22/05/15.
 */
public class RemovedEvent<K, V> implements MapEvent<K, V> {
    private final String assetName;
    private final K key;
    private final V value;

    private RemovedEvent(String assetName, K key, V value) {
        this.assetName = assetName;
        this.key = key;
        this.value = value;
    }

    @Override
    public String assetName() {
        return assetName;
    }

    @NotNull
    public static <K, V> RemovedEvent<K, V> of(String assetName, K key, V value) {
        return new RemovedEvent<>(assetName, key, value);
    }

    @NotNull
    @Override
    public <K2, V2> MapEvent<K2, V2> translate(@NotNull Function<K, K2> keyFunction, @NotNull Function<V, V2> valueFunction) {
        return new RemovedEvent<>(assetName, keyFunction.apply(key), valueFunction.apply(value));
    }

    @Override
    public <K2, V2> MapEvent<K2, V2> translate(BiFunction<K, K2, K2> keyFunction, BiFunction<V, V2, V2> valueFunction) {
        return new RemovedEvent<>(assetName, keyFunction.apply(key, null), valueFunction.apply(value, null));
    }

    public K key() {
        return key;
    }

    @Override
    public V oldValue() {
        return value;
    }

    @Nullable
    public V value() {
        return null;
    }
    @Override
    public void apply(@NotNull MapEventListener<K, V> listener) {
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
                .filter(e -> Objects.equals(assetName, e.assetName))
                .filter(e -> Objects.equals(key, e.key))
                .filter(e -> Objects.equals(value, e.value))
                .isPresent();
    }

    @Override
    public String toString() {
        return "RemovedEvent{" +
                "assetName='" + assetName + '\'' +
                ", key=" + key +
                ", value=" + value +
                '}';
    }
}
