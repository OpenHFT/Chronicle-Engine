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
public interface MapHandler {
    <V> BiConsumer<ValueOut, V> getKeyToWire();

    <V> Function<ValueIn, V> getWireToKey();

    <V> BiConsumer<ValueOut, V> getValueToWire();

    <V> Function<ValueIn, V> getWireToValue();

    <V> BiConsumer<ValueOut, Map.Entry<V, V>> getEntryToWire();

    <V> Function<ValueIn, Map.Entry<V, V>> getWireToEntry();

    static MapHandler create(StringBuilder csp) {
        if(csp.toString().contains("file")){
            return new FilePerKeyMapHandler();
        }
        return new ByteByteMapHandler();
    }

    Map getMap(ChronicleEngine engine, String serviceName) throws IOException;
}
