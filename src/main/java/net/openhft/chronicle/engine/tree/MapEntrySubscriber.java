package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.engine.api.Subscriber;

import java.util.Map;

/**
 * Created by peter.lawrey on 10/06/2015.
 */
public interface MapEntrySubscriber<K, V> extends Subscriber<Map.Entry<K, V>> {
}
