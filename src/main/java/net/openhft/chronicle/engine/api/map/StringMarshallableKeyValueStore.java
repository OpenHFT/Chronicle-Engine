package net.openhft.chronicle.engine.api.map;

import net.openhft.chronicle.engine.api.View;
import net.openhft.chronicle.wire.Marshallable;

/**
 * Created by peter on 25/05/15.
 */
public interface StringMarshallableKeyValueStore<V extends Marshallable> extends SubscriptionKeyValueStore<String, V, V>, View {
}
