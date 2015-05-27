package net.openhft.chronicle.engine2.api.map;

import net.openhft.chronicle.engine2.api.View;

/**
 * Created by peter on 25/05/15.
 */
public interface StringStringKeyValueStore extends SubscriptionKeyValueStore<String, StringBuilder, String>, View {
}
