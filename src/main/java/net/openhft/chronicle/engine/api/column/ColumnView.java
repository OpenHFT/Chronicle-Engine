package net.openhft.chronicle.engine.api.column;

import net.openhft.chronicle.engine.api.set.EntrySetView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.wire.AbstractMarshallable;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Rob Austin.
 */
public interface ColumnView<K> {

    ArrayList<String> columnNames();

    enum Type {
        key, value
    }

    class MarshableFilter extends AbstractMarshallable {
        public final Type type;
        public final Object value;

        public MarshableFilter(Type type, Object value) {
            this.type = type;
            this.value = value;
        }
    }

    class MarshableOrderBy extends AbstractMarshallable {
        public final String column;
        public final boolean isAscending;

        public MarshableOrderBy(String column, boolean isAscending) {
            this.column = column;
            this.isAscending = isAscending;
        }
    }


    class Query<K> extends AbstractMarshallable {
        public long fromIndex;
        public List<MarshableOrderBy> marshableOrderBy = new ArrayList<>();
        public List<MarshableFilter> marshableFilters = new ArrayList<>();

        public boolean filter(@NotNull Map.Entry<K, ?> entry) {
            for (MarshableFilter f : marshableFilters) {

                Object item;

                if (f.type == Type.key) {
                    item = entry.getKey();
                } else if (f.type == Type.value) {
                    item = entry.getValue();

                } else {
                    throw new UnsupportedOperationException();
                }

                if (!item.toString().toLowerCase().contains(f.value.toString().toLowerCase()))
                    return false;

            }
            return true;
        }


    }

    List<Column> columns();

    /**
     * @return the number of rows
     */
    long longSize();

    Asset asset();

    int size(Query<K> query);

    boolean containsKey(K k);

    Object remove(K key);

    /**
     * called when ever the user modify the cells and the data changes
     *
     * @param columnName the column name of the cell
     * @param key        the rowID of the cell
     * @param oldKey     the old rowID of the cell
     * @param value      the new value of the cell
     * @param oldValue   the old value of the cell
     */
    void onCellChanged(String columnName, K key, K oldKey, Object value, Object oldValue);

    EntrySetView<K, Object, ?> entrySet();

    /**
     * called whenever some data in the underlying stucture has changed and hence the visual
     * layer has to be refreshed
     *
     * @param r to refresh the visual layer
     */
    void onRefresh(@NotNull Runnable r);

    Iterator<Row> iterator(ColumnView.Query<K> query);

    boolean canDeleteRow();

    void addRow(K k, Object... v);

}
