package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.engine.client.internal.ChronicleEngine;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Created by daniel on 06/05/15.
 */
public class FilePerKeyMapHandler implements MapHandler {
    private final BiConsumer<ValueOut, String> keyToWire = ValueOut::object;

    private final Function<ValueIn, String> wireToKey =
            v -> v.object(String.class);

    private final BiConsumer<ValueOut, String> valueToWire = ValueOut::object;

    private final Function<ValueIn, String> wireToValue =
            v -> v.object(String.class);

    private final BiConsumer<ValueOut, Map.Entry<String, String>> entryToWire
            = (v, e) -> {
        v.marshallable(w -> {
            w.write(() -> "key").object(e.getKey()).write(() -> "value").object(e.getValue());
        });
    };

    private final Function<ValueIn, Map.Entry<String, String>> wireToEntry
            = valueIn -> valueIn.applyToMarshallable(x -> {

        final String key = x.read(() -> "key").object(String.class);
        final String value = x.read(() -> "value").object(String.class);

        return new Map.Entry<String, String>() {

            @Override
            public String getKey() {
                return key;
            }

            @Override
            public String getValue() {
                return value;
            }

            @Override
            public String setValue(String value) {
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

    public BiConsumer<ValueOut, String> getValueToWire() {
        return valueToWire;
    }

    public Function<ValueIn, String> getWireToValue() {
        return wireToValue;
    }

    public BiConsumer<ValueOut, Map.Entry<String, String>> getEntryToWire() {
        return entryToWire;
    }

    public Function<ValueIn, Map.Entry<String, String>> getWireToEntry() {
        return wireToEntry;
    }

    @Override
    public Map getMap(ChronicleEngine engine, String serviceName) throws IOException{
        return engine.getFilePerKeyMap(
                serviceName);
    }

}
