package net.openhft.chronicle.engine2.api.map;

import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.engine2.api.View;
import net.openhft.chronicle.wire.Marshallable;

/**
 * Created by peter on 25/05/15.
 */
public interface StringBytesMarshallableKeyValueStore<V extends BytesMarshallable> extends SubscriptionKeyValueStore<String, V, V>, View {
}
