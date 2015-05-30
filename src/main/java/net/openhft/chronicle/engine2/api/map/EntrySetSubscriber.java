package net.openhft.chronicle.engine2.api.map;

import net.openhft.chronicle.engine2.api.Subscriber;
import net.openhft.chronicle.engine2.api.View;

import java.util.Map;

/**
 * Created by peter on 30/05/15.
 */
public interface EntrySetSubscriber<K, V> extends Subscriber<Map.Entry<K, V>>, View {
}
