package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.ISubscriber;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;

/**
 * Created by peter.lawrey on 03/06/2015.
 */
public interface EventConsumer<K, V> extends ISubscriber {
    void notifyEvent(MapEvent<K, V> changeEvent) throws InvalidSubscriberException;

}
