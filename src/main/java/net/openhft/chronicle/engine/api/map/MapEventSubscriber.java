package net.openhft.chronicle.engine.api.map;

import net.openhft.chronicle.engine.api.Subscriber;
import net.openhft.chronicle.engine.api.View;

/**
 * Created by peter on 30/05/15.
 */
public interface MapEventSubscriber<K, V> extends Subscriber<ChangeEvent<K, V>>, View {
}
