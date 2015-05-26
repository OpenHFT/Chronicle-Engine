package net.openhft.chronicle.engine2.session;

import net.openhft.chronicle.engine2.api.View;
import net.openhft.chronicle.engine2.api.map.SubscriptionKeyValueStore;

/**
 * Created by peter on 25/05/15.
 */
public interface StringStringKeyValueStore extends SubscriptionKeyValueStore<String, StringBuilder, String>, View {
}
