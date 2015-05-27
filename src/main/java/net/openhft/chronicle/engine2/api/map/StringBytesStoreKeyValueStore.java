package net.openhft.chronicle.engine2.api.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;

/**
 * Created by peter on 25/05/15.
 */
public interface StringBytesStoreKeyValueStore extends SubscriptionKeyValueStore<String, Bytes, BytesStore> {
}
