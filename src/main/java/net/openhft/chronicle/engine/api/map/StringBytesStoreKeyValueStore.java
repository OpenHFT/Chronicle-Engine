package net.openhft.chronicle.engine.api.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.engine.api.tree.View;
import net.openhft.chronicle.engine.map.AuthenticatedKeyValueStore;

/**
 * Created by peter on 25/05/15.
 */
public interface StringBytesStoreKeyValueStore extends AuthenticatedKeyValueStore<String, Bytes, BytesStore>, View {
}
