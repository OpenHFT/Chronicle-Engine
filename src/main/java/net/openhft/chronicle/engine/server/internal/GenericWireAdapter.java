package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;

class GenericWireAdapter<K, V> implements WireAdapter<K, V> {

    private final BiConsumer<ValueOut, K> keyToWire = ValueOut::object;
    @Nullable
    private final Function<ValueIn, K> wireToKey;
    private final BiConsumer<ValueOut, V> valueToWire = ValueOut::object;
    @NotNull
    private final Function<ValueIn, V> wireToValue;
    @NotNull
    private final Function<ValueIn, Entry<K, V>> wireToEntry;

    GenericWireAdapter(@NotNull final Class<K> kClass, @NotNull final Class<V> vClass) {

        wireToKey = (valueIn) -> valueIn.object(kClass);
        wireToValue = in -> in.object(vClass);

        wireToEntry = valueIn -> valueIn.applyToMarshallable(x -> {

            final K key = x.read(() -> "key").object(kClass);
            final V value = x.read(() -> "value").object(vClass);

            return new Entry<K, V>() {
                @Nullable
                @Override
                public K getKey() {
                    return key;
                }

                @Nullable
                @Override
                public V getValue() {
                    return value;
                }

                @NotNull
                @Override
                public V setValue(V value) {
                    throw new UnsupportedOperationException();
                }
            };
        });
    }

    private final BiConsumer<ValueOut, Entry<K, V>> entryToWire
            = (v, e) -> v.marshallable(w -> w.write(() -> "key").object(e.getKey())
            .write(() -> "value").object(e.getValue()));

    public BiConsumer<ValueOut, K> keyToWire() {
        return keyToWire;
    }

    @Nullable
    public Function<ValueIn, K> wireToKey() {
        return wireToKey;
    }

    public BiConsumer<ValueOut, V> valueToWire() {
        return valueToWire;
    }

    @Nullable
    public Function<ValueIn, V> wireToValue() {
        return wireToValue;
    }

    @NotNull
    public BiConsumer<ValueOut, Entry<K, V>> entryToWire() {
        return entryToWire;
    }

    @NotNull
    public Function<ValueIn, Entry<K, V>> wireToEntry() {
        return wireToEntry;
    }

}
