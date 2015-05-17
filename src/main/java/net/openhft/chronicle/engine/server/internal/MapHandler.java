package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.client.internal.ChronicleEngine;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static net.openhft.chronicle.engine.client.StringUtils.contains;

/**
 * Created by daniel on 06/05/15.
 */
public interface MapHandler {

    MapHandler STRING_STRING_MAP_HANDLER = new StringStringMapHandler(
            (engine, serviceName) -> engine.getFilePerKeyMap(serviceName));
    MapHandler BYTE_BYTE_MAP_HANDLER = new ByteByteMapHandler();

    MapHandler STRING_CHAR_SEQUENCE_MAP_HANDLER = new StringCharSequenceMapHandler((engine, serviceName) -> {

        try {
            return engine.getMap(serviceName, String.class, CharSequence.class);
        } catch (IOException e) {
            throw Jvm.rethrow(e);
        }
    });

    <V> BiConsumer<ValueOut, V> getKeyToWire();

    <V> Function<ValueIn, V> getWireToKey();

    <V> BiConsumer<ValueOut, V> getValueToWire();

    <V> Function<ValueIn, V> getWireToValue();

    <V> BiConsumer<ValueOut, Map.Entry<V, V>> getEntryToWire();

    <V> Function<ValueIn, Map.Entry<V, V>> getWireToEntry();

    static MapHandler instance(CharSequence csp) {
        if (contains(csp, "file")) {
            return STRING_STRING_MAP_HANDLER;
        } else if (contains(csp, "object"))
            return BYTE_BYTE_MAP_HANDLER;
        else
            return STRING_CHAR_SEQUENCE_MAP_HANDLER;
    }

    Map getMap(ChronicleEngine engine, String serviceName) throws IOException;
}
