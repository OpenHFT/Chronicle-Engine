package net.openhft.chronicle.engine.api.map;

import net.openhft.chronicle.engine.api.Subscriber;
import net.openhft.chronicle.engine.api.View;

import java.util.Map;

/**
 * Created by peter on 30/05/15.
 */
public interface EntrySetSubscriber<K, V> extends Subscriber<Map.Entry<K, V>>, View {
}
