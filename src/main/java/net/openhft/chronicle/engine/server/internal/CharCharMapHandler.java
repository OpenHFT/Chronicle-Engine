package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.engine.client.internal.ChronicleEngine;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by daniel on 06/05/15.
 */
public class CharCharMapHandler implements MapHandler<CharSequence, CharSequence> {

    private final BiFunction<ChronicleEngine, String, ChronicleMap> supplier;

    CharCharMapHandler(@NotNull BiFunction<ChronicleEngine, String, ChronicleMap> supplier) {
        this.supplier = supplier;
    }

    StringBuilder keySb = new StringBuilder();
    StringBuilder valueSb = new StringBuilder();

    private final BiConsumer<ValueOut, CharSequence> keyToWire = ValueOut::object;

    private final BiConsumer<ValueOut, CharSequence> valueToWire = (valueOut, value) -> valueOut.object(value);

    private final Function<ValueIn, CharSequence> wireToKey = valueIn -> {
        keySb.setLength(1);
        valueIn.textTo(keySb);
        return keySb;
    };

    private final Function<ValueIn, CharSequence> wireToValue = valueIn -> {
        valueSb.setLength(1);
        valueIn.textTo(valueSb);
        return valueSb;
    };

    private final BiConsumer<ValueOut, Map.Entry<CharSequence, CharSequence>> entryToWire
            = (v, e) -> {
        v.marshallable(w -> {
            CharSequence key = e.getKey();
            CharSequence value = e.getValue();
            w.write(() -> "key").object(key).write(() -> "value").object(value);
        });
    };

    private final Function<ValueIn, Map.Entry<CharSequence, CharSequence>> wireToEntry
            = valueIn -> valueIn.applyToMarshallable(x -> {

        final CharSequence key = x.read(() -> "key").object(String.class);
        final CharSequence value = x.read(() -> "value").object(String.class);

        return new Map.Entry<CharSequence, CharSequence>() {
            @Override
            public CharSequence getKey() {
                return key;
            }

            @Override
            public CharSequence getValue() {
                return value;
            }

            @Override
            public CharSequence setValue(CharSequence value) {
                throw new UnsupportedOperationException();
            }
        };
    });

    public BiConsumer<ValueOut, CharSequence> getKeyToWire() {
        return keyToWire;
    }

    public Function<ValueIn, CharSequence> getWireToKey() {
        return wireToKey;
    }

    public BiConsumer<ValueOut, CharSequence> getValueToWire() {
        return valueToWire;
    }

    public Function<ValueIn, CharSequence> getWireToValue() {
        return wireToValue;
    }

    public BiConsumer<ValueOut, Map.Entry<CharSequence, CharSequence>> getEntryToWire() {
        return entryToWire;
    }

    public Function<ValueIn, Map.Entry<CharSequence, CharSequence>> getWireToEntry() {
        return wireToEntry;
    }

    @Override
    public ChronicleMap getMap(ChronicleEngine engine, String serviceName) throws IOException {
        return supplier.apply(engine, serviceName);
    }

    StringBuilder usingValue = new StringBuilder();

    @Override
    public StringBuilder usingValue() {
        usingValue.setLength(0);
        return usingValue;
    }
}
