/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

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
    private final BiConsumer<ValueOut, Entry<K, V>> entryToWire
            = (v, e) -> v.marshallable(w -> w.write(() -> "key").object(e.getKey())
            .write(() -> "value").object(e.getValue()));


    // if its a string builder re-uses it
    private final ThreadLocal<CharSequence> usingKey = ThreadLocal.withInitial(StringBuilder::new);
    private final ThreadLocal<CharSequence> usingValue = ThreadLocal.withInitial(StringBuilder::new);

    GenericWireAdapter(@NotNull final Class<K> kClass, @NotNull final Class<V> vClass) {

        wireToKey = (valueIn) -> valueIn.object(kClass);
        wireToValue = in -> in.object(vClass);
        wireToEntry = valueIn -> valueIn.applyToMarshallable(x -> {

            final K key = (K) ((kClass == CharSequence.class ) ?
                    x.read(() -> "key").object(usingKey.get(), CharSequence.class) :
                    x.read(() -> "key").object(kClass));

            final V value = (V) ((vClass == CharSequence.class) ?
                    x.read(() -> "value").object(usingValue.get(), CharSequence.class) :
                    x.read(() -> "value").object(vClass));

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

    @NotNull
    public BiConsumer<ValueOut, K> keyToWire() {
        return keyToWire;
    }

    @NotNull
    public Function<ValueIn, K> wireToKey() {
        return wireToKey;
    }

    @NotNull
    public BiConsumer<ValueOut, V> valueToWire() {
        return valueToWire;
    }

    @NotNull
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
