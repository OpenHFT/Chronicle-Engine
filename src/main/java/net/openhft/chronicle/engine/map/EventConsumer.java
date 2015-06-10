package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.map.MapReplicationEvent;

/**
 * Created by peter.lawrey on 03/06/2015.
 */
public interface EventConsumer<K, V> {
    void notifyEvent(MapReplicationEvent<K, V> mpe) throws InvalidSubscriberException;

}
