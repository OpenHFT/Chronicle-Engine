package net.openhft.chronicle.engine.api.set;

import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.Assetted;
import net.openhft.chronicle.engine.api.tree.View;

import java.util.Map;
import java.util.Set;

/**
 * Created by peter on 22/05/15.
 */
public interface EntrySetView<K, MV, V> extends Set<Map.Entry<K, V>>, Assetted<MapView<K, MV, V>>, View {
}
