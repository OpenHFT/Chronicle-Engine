package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.NativeBytes;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class StringISO8859MapHandler implements MapHandler<String, Bytes> {

    private final Function<String, Map> supplier;

    StringISO8859MapHandler(@NotNull Function<String, Map> supplier) {
        this.supplier = supplier;
    }

    private final BiConsumer<ValueOut, String> keyToWire = ValueOut::object;

    private final Function<ValueIn, String> wireToKey = ValueIn::text;

    private final BiConsumer<ValueOut, Bytes> valueToWire = ValueOut::object;

    final Bytes inBytes = NativeBytes.nativeBytes(2_500_000);
    private final Function<ValueIn, Bytes> wireToValue = in -> {
        in.textTo(inBytes);
        inBytes.flip();
        return inBytes;
    };

    private final BiConsumer<ValueOut,Entry<String, Bytes>> entryToWire
            = (v, e) -> {
        v.marshallable(w -> {
            w.write(() -> "key").object(e.getKey())
                    .write(() -> "value").text(e.getValue());
        });
    };

    private final Function<ValueIn,Entry<String, Bytes>> wireToEntry
            = valueIn -> valueIn.applyToMarshallable(x -> {

        final String key = x.read(() -> "key").object(String.class);
        x.read(() -> "value").textTo(inBytes);
        inBytes.flip();

        return new Entry<String, Bytes>() {
            @Override
            public String getKey() {
                return key;
            }

            @Override
            public Bytes getValue() {
                return inBytes;
            }

            @Override
            public Bytes setValue(Bytes value) {
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

    public BiConsumer<ValueOut, Bytes> getValueToWire() {
        return valueToWire;
    }

    public Function<ValueIn, Bytes> getWireToValue() {
        return wireToValue;
    }

    public BiConsumer<ValueOut, Entry<String, Bytes>> getEntryToWire() {
        return entryToWire;
    }

    public Function<ValueIn, Entry<String, Bytes>> getWireToEntry() {
        return wireToEntry;
    }

    @Override
    public Map getMap(String serviceName) throws IOException {
        return supplier.apply(serviceName);
    }

    @Override
    public Bytes usingValue() {
        return inBytes;
    }
}
