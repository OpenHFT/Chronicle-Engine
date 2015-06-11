package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.Asset;
import net.openhft.chronicle.engine.api.map.ChangeEvent;

/**
 * Created by peter.lawrey on 11/06/2015.
 */
public interface ObjectSubscription<K, MV, V> extends SubscriptionKVSCollection<K, MV, V> {
    <K, V> void notifyChildUpdate(Asset asset, ChangeEvent<K, V> changeEvent);
}
