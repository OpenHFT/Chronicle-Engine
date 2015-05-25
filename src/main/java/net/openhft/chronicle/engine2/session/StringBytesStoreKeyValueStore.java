package net.openhft.chronicle.engine2.session;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.engine2.api.map.SubscriptionKeyValueStore;

/**
 * Created by peter on 25/05/15.
 */
public interface StringBytesStoreKeyValueStore extends SubscriptionKeyValueStore<String, Bytes, BytesStore> {
}
