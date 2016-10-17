package net.openhft.chronicle.engine.api.column;

import net.openhft.chronicle.engine.api.set.EntrySetView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.wire.AbstractMarshallable;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * @author Rob Austin.
 */
public interface ColumnView<K> {

    public ArrayList<String> columnNames();

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

    public static class MarshableOrderBy extends AbstractMarshallable {
        public final String column;
        public final boolean isAscending;

        public MarshableOrderBy(String column, boolean isAscending) {
            this.column = column;
            this.isAscending = isAscending;
        }
    }


    class Query<K> {
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

        public Comparator<Map.Entry<K, ?>> sorted() {
            return this::compare;
        }

        private int compare(Map.Entry<K, ?> o1, Map.Entry<K, ?> o2) {
            for (MarshableOrderBy order : marshableOrderBy) {

                int result = 0;
                if ("key".equals(order.column))
                    if (o1.getKey() instanceof Number)
                        result = ((Comparable) o1.getKey()).compareTo(o2.getKey());
                    else
                        result = ((Comparable) o1.getKey().toString().toLowerCase()).compareTo(o2.getKey().toString().toLowerCase());

                else if ("value".equals(order.column))
                    if (o1.getValue() instanceof Number)
                        result = ((Comparable) o1.getValue()).compareTo(o2.getValue());
                    else
                        result = ((Comparable) o1.getValue().toString().toLowerCase()).compareTo(o2.getValue().toString().toLowerCase());
                result *= order.isAscending ? 1 : -1;
                if (result != 0)
                    return result;

            }
            return 0;
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
