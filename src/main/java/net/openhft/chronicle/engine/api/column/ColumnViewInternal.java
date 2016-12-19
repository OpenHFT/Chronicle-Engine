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

    default boolean toRange(Number n, @NotNull String value) {

        if (value.contains(",")) {
            String[] v = value.split("\\,");
            if (v.length != 2)
                return DOp.toRange(n, value, true);

            boolean result = true;
            for (String v0 : v) {
                String v1 = v0.trim();
                boolean isAtStart = !(v1.endsWith("]") || v1.startsWith(")"));
                result = result && DOp.toRange(n, v1, isAtStart);
                if (!result)
                    return result;
            }
            return true;


        } else
            return DOp.toRange(n, value, true);
    }



    enum DOp {
        GE(true, ">=", "[") {
            @Override
            boolean compare(double a, double b) {
                return a >= b;
            }
        },
        LE(true, "<=") {
            @Override
            boolean compare(double a, double b) {
                return a <= b;
            }
        },
        NE(true, "<>", "!=", "!") {
            @Override
            boolean compare(double a, double b) {
                return a != b;
            }
        },
        GT(true, ">", "(") {
            @Override
            boolean compare(double a, double b) {
                return a > b;
            }
        },
        LT(true, "<") {
            @Override
            boolean compare(double a, double b) {
                return a < b;
            }
        },
        EQ(true, "==", "=", "") {
            @Override
            boolean compare(double a, double b) {
                return a == b;
            }
        },

        LT_INCLUSIVE(false, "]") {
            @Override
            boolean compare(double a, double b) {
                return a <= b;
            }
        },

        LT_EXCLUSIVE(false, ")") {
            @Override
            boolean compare(double a, double b) {
                return a < b;
            }
        };

        static final DOp[] OPS = values();
        final String[] op;
        private final boolean operationAtStart;

        DOp(boolean operationAtStart, String... op) {
            this.op = op;
            this.operationAtStart = operationAtStart;
        }


        private Number number(String op, String value, Class<? extends Number> clazz) {
            @NotNull final String number;


            number = (operationAtStart)
                    ? value.substring(op.length()).trim()
                    : value.substring(0, value.length() - op.length()).trim();
            return convertTo(clazz, number);
        }

        abstract boolean compare(double a, double b);

        /**
         * @param o
         * @param value
         * @param operationAtStart if true the first character is expected to be the operation, otherwise the last character is ex
         * @return
         */
        public static boolean toRange(@NotNull Number o, @NotNull String value, boolean operationAtStart) {

            for (DOp dop : DOp.OPS) {
                if (dop.operationAtStart != operationAtStart)
                    continue;

                for (String op : dop.op) {

                    if (!dop.isValid(value, op))
                        continue;

                    final Number number = dop.number(op, value.trim(), o.getClass());

                    try {
                        final Number filterNumber = convertTo(o.getClass(), number);
                        return dop.compare(o.doubleValue(), filterNumber.doubleValue());
                    } catch (ClassCastException e) {
                        return false;
                    }

                }
            }
            return false;
        }

        private boolean isValid(String value, String op) {
            return operationAtStart ? value.startsWith(op) : value.endsWith(op);
        }


    }
}



