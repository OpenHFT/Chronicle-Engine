package net.openhft.chronicle.engine.api.map;

import net.openhft.chronicle.engine.api.View;

/**
 * Created by peter on 25/05/15.
 */
public interface StringStringKeyValueStore extends SubscriptionKeyValueStore<String, StringBuilder, String>, View {
}
