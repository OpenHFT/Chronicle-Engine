package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.engine.client.internal.ChronicleEngine;
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
public class CharCharMapHandler implements MapHandler {

    private final BiFunction<ChronicleEngine, String, Map> supplier;

    CharCharMapHandler(@NotNull BiFunction<ChronicleEngine, String, Map> supplier) {
        this.supplier = supplier;
    }


    StringBuilder keySb = new StringBuilder();

    private final BiConsumer<ValueOut, CharSequence> keyToWire = ValueOut::object;

    private final Function<ValueIn, CharSequence> wireToKey = (valueIn) -> {
     //   valueIn.text(keySb);
        return valueIn.text();
    };

    private final BiConsumer<ValueOut, CharSequence> valueToWire = ValueOut::object;

    private final Function<ValueIn, CharSequence> wireToValue = ValueIn::text;

    private final BiConsumer<ValueOut, Map.Entry<CharSequence, CharSequence>> entryToWire
            = (v, e) -> {
        v.marshallable(w -> {
            w.write(() -> "key").object(e.getKey()).write(() -> "value").object(e.getValue());
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
    public Map getMap(ChronicleEngine engine, String serviceName) throws IOException {
        return supplier.apply(engine, serviceName);
    }

}
