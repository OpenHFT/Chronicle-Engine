package net.openhft.chronicle.engine.api.map;

import net.openhft.chronicle.engine.api.Subscriber;
import net.openhft.chronicle.engine.api.View;

/**
 * Created by peter on 30/05/15.
 */
public interface KeySubscriber<K> extends Subscriber<K>, View {
}
