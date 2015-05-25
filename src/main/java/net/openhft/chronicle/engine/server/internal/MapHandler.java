package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.client.internal.ChronicleEngine;
import net.openhft.chronicle.engine.map.MapHandlerFunction;

import java.io.IOException;
import java.util.Map;

import static net.openhft.chronicle.core.util.StringUtils.contains;

/**
 * Created by daniel on 06/05/15.
 */
public interface MapHandler<K, V> extends MapHandlerFunction<K, V> {

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

    MapHandler STRING_ISO8859_MAP_HANDLER = new StringISO8859MapHandler((engine, serviceName) -> {

        try {
            return engine.getMap(serviceName, String.class, CharSequence.class);
        } catch (IOException e) {
            throw Jvm.rethrow(e);
        }
    });

    static MapHandler instance(CharSequence csp) {
        if (contains(csp, "file")) {
            return STRING_STRING_MAP_HANDLER;

        } else if (contains(csp, "object"))
            return BYTE_BYTE_MAP_HANDLER;
        else
            return STRING_CHAR_SEQUENCE_MAP_HANDLER;
    }

    Map<K, V> getMap(ChronicleEngine engine, String serviceName) throws IOException;
}
