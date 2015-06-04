package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;

import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class GenericMapHandler<K, V> implements MapHandler<K, V> {

    private final BiConsumer<ValueOut, K> keyToWire = ValueOut::object;
    private final Function<ValueIn, K> wireToKey;
    private final BiConsumer<ValueOut, V> valueToWire = ValueOut::object;
    private final Function<ValueIn, V> wireToValue;
    private final Function<ValueIn, Entry<K, V>> wireToEntry;

    GenericMapHandler(final Class<K> kClass, final Class<V> vClass) {

        wireToKey = (valueIn) -> valueIn.object(kClass);
        wireToValue = in -> in.object(vClass);

        wireToEntry
                = valueIn -> valueIn.applyToMarshallable(x -> {

            final K key = x.read(() -> "key").object(kClass);
            final V value = x.read(() -> "value").object(vClass);

            return new Entry<K, V>() {
                @Override
                public K getKey() {
                    return key;
                }

                @Override
                public V getValue() {
                    return value;
                }

                @Override
                public V setValue(V value) {
                    throw new UnsupportedOperationException();
                }
            };
        });
    }

    private final BiConsumer<ValueOut, Entry<K, V>> entryToWire
            = (v, e) -> {
        v.marshallable(w -> {
            w.write(() -> "key").object(e.getKey())
                    .write(() -> "value").object(e.getValue());
        });
    };

    public BiConsumer<ValueOut, K> getKeyToWire() {
        return keyToWire;
    }

    public Function<ValueIn, K> getWireToKey() {
        return wireToKey;
    }

    public BiConsumer<ValueOut, V> getValueToWire() {
        return valueToWire;
    }

    public Function<ValueIn, V> getWireToValue() {
        return wireToValue;
    }

    public BiConsumer<ValueOut, Entry<K, V>> getEntryToWire() {
        return entryToWire;
    }

    public Function<ValueIn, Entry<K, V>> getWireToEntry() {
        return wireToEntry;
    }

}
