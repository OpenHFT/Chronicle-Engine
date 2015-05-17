package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.engine.client.internal.ChronicleEngine;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by daniel on 06/05/15.
 */
public class StringCharSequenceMapHandler implements MapHandler<String, CharSequence> {

    private final BiFunction<ChronicleEngine, String, Map> supplier;

    StringCharSequenceMapHandler(@NotNull BiFunction<ChronicleEngine, String, Map> supplier) {
        this.supplier = supplier;
    }

    private final BiConsumer<ValueOut, String> keyToWire = ValueOut::object;

    private final Function<ValueIn, String> wireToKey = ValueIn::text;

    private final BiConsumer<ValueOut, CharSequence> valueToWire = ValueOut::object;

    private final Function<ValueIn, CharSequence> wireToValue = in -> {
        StringBuilder sb = Wires.acquireStringBuilder();
        in.text(sb);
        return sb;
    };

    private final BiConsumer<ValueOut, Map.Entry<String, CharSequence>> entryToWire
            = (v, e) -> {
        v.marshallable(w -> {
            w.write(() -> "key").object(e.getKey())
                    .write(() -> "value").text(e.getValue());
        });
    };

    private final Function<ValueIn, Map.Entry<String, CharSequence>> wireToEntry
            = valueIn -> valueIn.applyToMarshallable(x -> {

        final String key = x.read(() -> "key").object(String.class);
        final StringBuilder value = Wires.acquireStringBuilder();
        x.read(() -> "value").text(value);

        return new Map.Entry<String, CharSequence>() {

            @Override
            public String getKey() {
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

    public BiConsumer<ValueOut, String> getKeyToWire() {
        return keyToWire;
    }

    public Function<ValueIn, String> getWireToKey() {
        return wireToKey;
    }

    public BiConsumer<ValueOut, CharSequence> getValueToWire() {
        return valueToWire;
    }

    public Function<ValueIn, CharSequence> getWireToValue() {
        return wireToValue;
    }

    public BiConsumer<ValueOut, Map.Entry<String, CharSequence>> getEntryToWire() {
        return entryToWire;
    }

    public Function<ValueIn, Map.Entry<String, CharSequence>> getWireToEntry() {
        return wireToEntry;
    }

    @Override
    public Map getMap(ChronicleEngine engine, String serviceName) throws IOException {
        return supplier.apply(engine, serviceName);
    }

    @Override
    public StringBuilder usingValue() {
        return Wires.acquireStringBuilder();
    }
}
