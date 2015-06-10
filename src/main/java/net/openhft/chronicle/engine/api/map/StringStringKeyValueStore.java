package net.openhft.chronicle.engine.api.map;

import net.openhft.chronicle.engine.api.View;
import net.openhft.chronicle.engine.map.ObjectKeyValueStore;

/**
 * Created by peter on 25/05/15.
 */
public interface StringStringKeyValueStore extends ObjectKeyValueStore<String, StringBuilder, String>, View {
}
