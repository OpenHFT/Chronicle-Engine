package net.openhft.chronicle.engine.api.column;

import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.set.EntrySetView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.map.VaadinLambda;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Rob Austin.
 */
public interface ColumnView<K> {

    List<Column> columns();

    /**
     * @return the number of rows
     */
    long longSize();

    Asset asset();

    int size(VaadinLambda.Query<K> query);

    boolean containsKey(K k);

    Object remove(K key);

    void onCellChanged(String columnName, K key, K oldKey, Object value, Object oldValue);

    EntrySetView<K, Object, ?> entrySet();

    void onCellChanged(@NotNull Subscriber<MapEvent<K, ?>> subscriber);

    Iterator<? extends Map.Entry<K, ?>> iterator(VaadinLambda.Query<K> query);

    boolean canDeleteRow();

    void addRow(K k, Object... v);
}
