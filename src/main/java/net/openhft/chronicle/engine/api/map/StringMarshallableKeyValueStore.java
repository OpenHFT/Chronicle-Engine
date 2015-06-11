package net.openhft.chronicle.engine.api.map;

import net.openhft.chronicle.engine.api.tree.View;
import net.openhft.chronicle.engine.map.ObjectKeyValueStore;
import net.openhft.chronicle.wire.Marshallable;

/**
 * Created by peter on 25/05/15.
 */
public interface StringMarshallableKeyValueStore<V extends Marshallable> extends ObjectKeyValueStore<String, V, V>, View {
}
