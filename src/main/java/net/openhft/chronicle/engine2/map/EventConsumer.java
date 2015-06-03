package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.InvalidSubscriberException;
import net.openhft.chronicle.engine2.api.map.MapReplicationEvent;

/**
 * Created by peter.lawrey on 03/06/2015.
 */
public interface EventConsumer<K, V> {
    void notifyEvent(MapReplicationEvent<K, V> mpe) throws InvalidSubscriberException;
}
