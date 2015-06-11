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
public class InsertedEvent<K, V> implements MapEvent<K, V> {
    private final String assetName;
    private final K key;
    private final V value;

    private InsertedEvent(String assetName, K key, V value) {
        this.assetName = assetName;
        this.key = key;
        this.value = value;
    }

    @NotNull
    public static <K, V> InsertedEvent<K, V> of(String assetName, K key, V value) {
        return new InsertedEvent<>(assetName, key, value);
    }

    @NotNull
    @Override
    public <K2, V2> MapEvent<K2, V2> translate(@NotNull Function<K, K2> keyFunction, @NotNull Function<V, V2> valueFunction) {
        return new InsertedEvent<>(assetName, keyFunction.apply(key), valueFunction.apply(value));
    }

    @Override
    public <K2, V2> MapEvent<K2, V2> translate(BiFunction<K, K2, K2> keyFunction, BiFunction<V, V2, V2> valueFunction) {
        return new InsertedEvent<>(assetName, keyFunction.apply(key, null), valueFunction.apply(value, null));
    }

    public K key() {
        return key;
    }

    @Nullable
    @Override
    public V oldValue() {
        return null;
    }

    public V value() {
        return value;
    }

    @Override
    public void apply(@NotNull MapEventListener<K, V> listener) {
        listener.insert(key, value);
    }

    @Override
    public int hashCode() {
        return Objects.hash("inserted", key, value);
    }

    @Override
    public boolean equals(Object obj) {
        return Optional.ofNullable(obj)
                .filter(o -> o instanceof InsertedEvent)
                .map(o -> (InsertedEvent<K, V>) o)
                .filter(e -> Objects.equals(assetName, e.assetName))
                .filter(e -> Objects.equals(key, e.key))
                .filter(e -> Objects.equals(value, e.value))
                .isPresent();
    }

    @Override
    public String toString() {
        return "InsertedEvent{" +
                "assetName='" + assetName + '\'' +
                ", key=" + key +
                ", value=" + value +
                '}';
    }

    @Override
    public String assetName() {
        return assetName;
    }
}
