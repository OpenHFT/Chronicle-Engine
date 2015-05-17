package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.client.internal.ChronicleEngine;
import net.openhft.chronicle.wire.map.MapHandlerFunction;

import java.io.IOException;
import java.util.Map;

import static net.openhft.chronicle.engine.client.StringUtils.contains;

/**
 * Created by daniel on 06/05/15.
 */
public interface MapHandler<M extends Map> extends MapHandlerFunction {

    MapHandler STRING_STRING_MAP_HANDLER = new StringStringMapHandler((engine, serviceName) ->
            engine.getFilePerKeyMap(serviceName));
    MapHandler BYTE_BYTE_MAP_HANDLER = new ByteByteMapHandler();


    MapHandler CHAR_CHAR_MAP_HANDLER = new CharCharMapHandler((engine, serviceName) -> {

        try {
            return engine.getMap(serviceName, CharSequence.class, CharSequence.class);
        } catch (IOException e) {
            Jvm.rethrow(e);
            // keeps the compiler happy :-)
            return null;
        }

    });

    static MapHandler instance(CharSequence csp) {

        if (contains(csp, "file")) {
            return STRING_STRING_MAP_HANDLER;
        } else if (contains(csp, "object"))
            return BYTE_BYTE_MAP_HANDLER;
        else
            return CHAR_CHAR_MAP_HANDLER;

    }

    M getMap(ChronicleEngine engine, String serviceName) throws IOException;


}
