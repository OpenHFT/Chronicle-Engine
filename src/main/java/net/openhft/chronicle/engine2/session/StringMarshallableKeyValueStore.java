package net.openhft.chronicle.engine2.session;

import net.openhft.chronicle.engine2.api.View;
import net.openhft.chronicle.engine2.api.map.SubscriptionKeyValueStore;
import net.openhft.chronicle.wire.Marshallable;

/**
 * Created by peter on 25/05/15.
 */
public interface StringMarshallableKeyValueStore<V extends Marshallable> extends SubscriptionKeyValueStore<String, V, V>, View {
}
