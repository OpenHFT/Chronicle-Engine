package net.openhft.chronicle.engine2.api.set;

import net.openhft.chronicle.engine2.api.Assetted;
import net.openhft.chronicle.engine2.api.View;
import net.openhft.chronicle.engine2.api.map.MapView;

import java.util.Map;
import java.util.Set;

/**
 * Created by peter on 22/05/15.
 */
public interface EntrySetView<K, MV, V> extends Set<Map.Entry<K, V>>, Assetted<MapView<K, MV, V>>, View {
}
