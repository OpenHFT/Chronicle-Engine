package net.openhft.chronicle.engine2.api.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;

/**
 * A KeyValue store where the key and value are accessible as Bytes.
 * <p>
 * Created by peter on 25/05/15.
 */
public interface BytesKeyValueStore<K> extends SubscriptionKeyValueStore<K, Bytes, BytesStore> {
}
