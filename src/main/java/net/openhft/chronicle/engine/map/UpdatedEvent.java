package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapEventListener;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by peter on 22/05/15.
 */
public class UpdatedEvent<K, V> implements MapEvent<K, V> {
    private String assetName;
    private K key;
    private V oldValue;
    private V value;

    private UpdatedEvent(String assetName, K key, V oldValue, V value) {
        this.assetName = assetName;
        this.key = key;
        this.oldValue = oldValue;
        this.value = value;
    }

    @NotNull
    public static <K, V> UpdatedEvent<K, V> of(String assetName, K key, V oldValue, V value) {
        return new UpdatedEvent<>(assetName, key, oldValue, value);
    }

    @NotNull
    @Override
    public <K2, V2> MapEvent<K2, V2> translate(@NotNull Function<K, K2> keyFunction, @NotNull Function<V, V2> valueFunction) {
        return new UpdatedEvent<>(assetName, keyFunction.apply(key), valueFunction.apply(oldValue), valueFunction.apply(value));
    }

    @Override
    public <K2, V2> MapEvent<K2, V2> translate(BiFunction<K, K2, K2> keyFunction, BiFunction<V, V2, V2> valueFunction) {
        return new UpdatedEvent<>(assetName, keyFunction.apply(key, null), valueFunction.apply(oldValue, null), valueFunction.apply(value, null));
    }

    @Override
    public String assetName() {
        return assetName;
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
                .filter(e -> Objects.equals(assetName, e.assetName))
                .filter(e -> Objects.equals(key, e.key))
                .filter(e -> Objects.equals(oldValue, e.oldValue))
                .filter(e -> Objects.equals(value, e.value))
                .isPresent();
    }

    @Override
    public String toString() {
        return "UpdatedEvent{" +
                "assetName='" + assetName + '\'' +
                ", key=" + key +
                ", oldValue=" + oldValue +
                ", value=" + value +
                '}';
    }

    @Override
    public void readMarshallable(WireIn wire) throws IllegalStateException {
        wire.read(MapEventFields.assetName).text(s -> assetName = s);
        key = (K) wire.read(MapEventFields.key).object(Object.class);
        oldValue = (V) wire.read(MapEventFields.oldValue).object(Object.class);
        value = (V) wire.read(MapEventFields.value).object(Object.class);
    }

    @Override
    public void writeMarshallable(WireOut wire) {
        wire.write(MapEventFields.assetName).text(assetName);
        wire.write(MapEventFields.key).object(key);
        wire.write(MapEventFields.oldValue).object(oldValue);
        wire.write(MapEventFields.value).object(value);
    }
}
