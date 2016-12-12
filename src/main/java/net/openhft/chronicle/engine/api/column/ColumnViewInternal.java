package net.openhft.chronicle.engine.api.column;

import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.map.ObjectSubscription;
import net.openhft.chronicle.wire.AbstractMarshallable;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static net.openhft.chronicle.core.util.ObjectUtils.convertTo;

/**
 * @author Rob Austin.
 */
public interface ColumnViewInternal {

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

        public MarshableOrderBy(String column) {
            this.column = column;
            this.isAscending = false;
        }

        public MarshableOrderBy(String column, boolean isAscending) {
            this.column = column;
            this.isAscending = isAscending;
        }
    }

    class SortedFilter extends AbstractMarshallable {
        public long countFromEnd;
        public long fromIndex;
        public List<MarshableOrderBy> marshableOrderBy = new ArrayList<>();
        public List<MarshableFilter> marshableFilters = new ArrayList<>();
    }

    Asset asset();

    List<Column> columns();

    int rowCount(@NotNull SortedFilter sortedFilter);

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

    ClosableIterator<? extends Row> iterator(@NotNull SortedFilter sortedFilter);

    boolean canDeleteRows();

    boolean containsRowWithKey(List keys);

    ObjectSubscription objectSubscription();


    enum DOp {
        GE(">=") {
            @Override
            boolean compare(double a, double b) {
                return a >= b;
            }
        },
        LE("<=") {
            @Override
            boolean compare(double a, double b) {
                return a <= b;
            }
        },
        NE("<>", "!=", "!") {
            @Override
            boolean compare(double a, double b) {
                return a != b;
            }
        },
        GT(">") {
            @Override
            boolean compare(double a, double b) {
                return a > b;
            }
        },
        LT("<") {
            @Override
            boolean compare(double a, double b) {
                return a < b;
            }
        },
        EQ("==", "=", "") {
            @Override
            boolean compare(double a, double b) {
                return a == b;
            }
        };

        static final DOp[] OPS = values();
        final String[] op;

        DOp(String... op) {
            this.op = op;
        }

        abstract boolean compare(double a, double b);

        public static boolean toRange(@NotNull Number o, @NotNull String trimmed) {
            for (DOp dop : DOp.OPS) {
                for (String op : dop.op) {
                    if (trimmed.startsWith(op)) {
                        @NotNull final String number = trimmed.substring(op.length()).trim();

                        try {
                            final Number filterNumber = convertTo(o.getClass(), number);
                            return dop.compare(o.doubleValue(), filterNumber.doubleValue());
                        } catch (ClassCastException e) {
                            return false;
                        }
                    }
                }
            }
            return false;
        }
    }
}
