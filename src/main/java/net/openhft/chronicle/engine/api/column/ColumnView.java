package net.openhft.chronicle.engine.api.column;

import net.openhft.chronicle.engine.map.ObjectSubscription;
import net.openhft.chronicle.wire.AbstractMarshallable;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Rob Austin.
 */
public interface ColumnView {

    class MarshableFilter extends AbstractMarshallable {
        public final String columnName;
        public final String filter;

        public MarshableFilter(String columnName, String filter) {
            this.columnName = columnName;
            this.filter = filter;
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

    class SortedFilter extends AbstractMarshallable {
        public long fromIndex;
        public final List<MarshableOrderBy> marshableOrderBy = new ArrayList<>();
        public final List<MarshableFilter> marshableFilters = new ArrayList<>();
    }

    List<Column> columns();

    int rowCount(@NotNull List<MarshableFilter> sortedFilter);

    /**
     * used to add, update and delete rows
     * called when ever the user modify the cells and the data changes
     *
     * @param row    if {@code row} is empty, the row is removed, based on the key in  {@code
     *               oldRow}
     * @param oldRow if {@code oldRow} is empty, the row is added , based on the key in {@code row}
     * @return the number of rows effected
     */
    int changedRow(@NotNull Map<String, Object> row, @NotNull Map<String, Object> oldRow);

    /**
     * called whenever some data in the underlying structure has changed and hence the visual
     * layer has to be refreshed
     *
     * @param r to refresh the visual layer
     */
    void registerChangeListener(@NotNull Runnable r);

    Iterator<? extends Row> iterator(@NotNull SortedFilter sortedFilter);

    boolean canDeleteRows();

    boolean containsRowWithKey(Object[] keys);

    ObjectSubscription objectSubscription();

}
