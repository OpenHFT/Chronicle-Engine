package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.engine.map.MapHandlerFunction;
import net.openhft.chronicle.engine.Chassis;

import java.io.IOException;
import java.util.Map;

/**
 * Created by daniel on 06/05/15.
 */
public interface MapHandler<K, V> extends MapHandlerFunction<K, V> {

    MapHandler STRING_CHAR_SEQUENCE_MAP_HANDLER = new StringCharSequenceMapHandler((
            serviceName)
            -> Chassis.acquireMap(serviceName, String.class, CharSequence.class));

    MapHandler STRING_STING_SEQUENCE_MAP_HANDLER = new StringStringMapHandler((
            serviceName) -> Chassis.acquireMap(serviceName, String.class, String.class));

    static MapHandler instance(CharSequence csp) {
        return STRING_STING_SEQUENCE_MAP_HANDLER;
    }

    Map<K, V> getMap(String serviceName) throws IOException;

}
