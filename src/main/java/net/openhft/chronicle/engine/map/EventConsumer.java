package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.ISubscriber;
import net.openhft.chronicle.engine.api.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.map.MapEvent;

/**
 * Created by peter.lawrey on 03/06/2015.
 */
public interface EventConsumer<K, V> extends ISubscriber {
    void notifyEvent(MapEvent<K, V> mp) throws InvalidSubscriberException;

}
