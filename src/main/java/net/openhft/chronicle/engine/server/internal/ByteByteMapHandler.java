package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.engine.client.internal.ChronicleEngine;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Created by daniel on 06/05/15.
 */
public class ByteByteMapHandler implements MapHandler<byte[], byte[]> {
    private final BiConsumer<ValueOut, byte[]> keyToWire = ValueOut::object;

    private final Function<ValueIn, byte[]> wireToKey =
            v -> v.object(byte[].class);

    private final BiConsumer<ValueOut, byte[]> valueToWire = ValueOut::object;

    private final Function<ValueIn, byte[]> wireToValue =
            v -> v.object(byte[].class);

    private final BiConsumer<ValueOut, Map.Entry<byte[], byte[]>> entryToWire
            = (v, e) -> {
        v.marshallable(w -> {
            w.write(() -> "key").object(e.getKey()).write(() -> "value").object(e.getValue());
        });
    };

    private final Function<ValueIn, Map.Entry<byte[], byte[]>> wireToEntry
            = valueIn -> valueIn.applyToMarshallable(x -> {

        final byte[] key = x.read(() -> "key").object(byte[].class);
        final byte[] value = x.read(() -> "value").object(byte[].class);

        return new Map.Entry<byte[], byte[]>() {
            @Override
            public byte[] getKey() {
                return key;
            }

            @Override
            public byte[] getValue() {
                return value;
            }

            @Override
            public byte[] setValue(byte[] value) {
                throw new UnsupportedOperationException();
            }
        };
    });

    public BiConsumer<ValueOut, byte[]> getKeyToWire() {
        return keyToWire;
    }

    public Function<ValueIn, byte[]> getWireToKey() {
        return wireToKey;
    }

    public BiConsumer<ValueOut, byte[]> getValueToWire() {
        return valueToWire;
    }

    public Function<ValueIn, byte[]> getWireToValue() {
        return wireToValue;
    }

    public BiConsumer<ValueOut, Map.Entry<byte[], byte[]>> getEntryToWire() {
        return entryToWire;
    }

    public Function<ValueIn, Map.Entry<byte[], byte[]>> getWireToEntry() {
        return wireToEntry;
    }

    @Override
    public ChronicleMap<byte[], byte[]> getMap(ChronicleEngine engine, String serviceName) throws IOException {
        return engine.getMap(
                serviceName,
                byte[].class,
                byte[].class);
    }

    @Override
    public byte[] usingValue() {
        return null;
    }
}
