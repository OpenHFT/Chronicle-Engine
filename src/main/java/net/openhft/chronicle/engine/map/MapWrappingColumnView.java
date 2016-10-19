package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.column.Column;
import net.openhft.chronicle.engine.api.column.ColumnView;
import net.openhft.chronicle.engine.api.column.Row;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Rob Austin.
 */
public class MapWrappingColumnView<K, V> implements ColumnView {


    public MapWrappingColumnView(RequestContext requestContext, Asset asset, MapView<K, V> u) {

    }

    @Override
    public List<Column> columns() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public int rowCount(@Nullable Query query) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public int changedRow(@NotNull Map<String, Object> row, @NotNull Map<String, Object> oldRow) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void registerChangeListener(@NotNull Runnable r) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Iterator<Row> iterator(Query query) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public boolean canDeleteRows() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public boolean containsRowWithKey(Object[] keys) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public ObjectSubscription objectSubscription() {
        throw new UnsupportedOperationException("todo");
    }
}