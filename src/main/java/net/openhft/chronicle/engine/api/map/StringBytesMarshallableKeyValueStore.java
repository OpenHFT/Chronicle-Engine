package net.openhft.chronicle.engine.api.map;

import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.engine.api.tree.View;

/**
 * Created by peter on 25/05/15.
 */
public interface StringBytesMarshallableKeyValueStore<V extends BytesMarshallable> extends SubscriptionKeyValueStore<String, V, V>, View {
}
