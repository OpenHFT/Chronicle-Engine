package net.openhft.chronicle.engine.api.column;

import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.map.ObjectSubscription;
import net.openhft.chronicle.wire.AbstractMarshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

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
        @NotNull
        public List<MarshableOrderBy> marshableOrderBy = new ArrayList<>();
        @NotNull
        public List<MarshableFilter> marshableFilters = new ArrayList<>();
    }

    Asset asset();

    @Nullable
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

    @NotNull
    ClosableIterator<? extends Row> iterator(@NotNull SortedFilter sortedFilter);

    boolean canDeleteRows();

    boolean containsRowWithKey(List keys);

    @Nullable
    ObjectSubscription objectSubscription();


    @Nullable
    default Predicate<Number> toPredicate(@NotNull String value) {
        if (value.contains(",")) {
            @NotNull String[] v = value.split("\\,");
            // if (v.length != 2)
            //    return DOp.toPredicate(value, true);


            @Nullable Predicate<Number> predicate = null;
            for (@NotNull String x : v) {
                @NotNull String xTrimed = x.trim();

                xTrimed = DOp.checkShouldPrependEQ(xTrimed);

                @Nullable Boolean atStart = DOp.isAtStart(xTrimed);
                if (atStart == null)
                    continue;

                predicate = (predicate == null)
                        ? DOp.toPredicate(xTrimed, atStart)
                        : predicate.and(DOp.toPredicate(xTrimed, atStart));

            }
            return predicate;

        } else {
            value = DOp.checkShouldPrependEQ(value);
            @Nullable Boolean atStart = DOp.isAtStart(value);
            return DOp.toPredicate(value, atStart);
        }
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
        EQ(true, "==", "=") {
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


        @Nullable
        private Number number(@NotNull String op, @NotNull String value, Class<? extends Number> clazz) throws Exception {
            @NotNull final String number;


            number = (operationAtStart)
                    ? value.substring(op.length()).trim()
                    : value.substring(0, value.length() - op.length()).trim();
            if (!number.isEmpty())
                return convertTo(clazz, number);

            throw new RuntimeException("can not parse number from '" + value + "'");

        }

        abstract boolean compare(double a, double b);

        private static Boolean isAtStart(@NotNull String value) {
            for (@NotNull DOp dop : DOp.OPS) {

                for (@NotNull String op : dop.op) {
                    if (dop.operationAtStart) {
                        if (value.startsWith(op))
                            return true;
                    } else {
                        if (value.endsWith(op))
                            return false;
                    }

                }


            }

            return null;
        }

        @NotNull
        private static String checkShouldPrependEQ(@NotNull String x) {
            return (isAtStart(x) == null) ? "=" + x : x;
        }

        /**
         * @param value
         * @param operationAtStart if true the first character is expected to be the operation,
         *                         otherwise the last character is ex
         * @return a  Predicate<Number>
         */
        private static Predicate<Number> toPredicate(@NotNull String value, boolean operationAtStart) {


            for (@NotNull DOp dop : DOp.OPS) {
                if (dop.operationAtStart != operationAtStart)
                    continue;

                for (@NotNull String op : dop.op) {


                    if (dop.operationAtStart) {
                        if (!value.startsWith(op))
                            continue;
                    } else {
                        if (!value.endsWith(op))
                            continue;
                    }

                    @Nullable final Number number;
                    try {
                        number = dop.number(op, value.trim(), Double.class);
                    } catch (Exception e) {
                        return n -> false;
                    }

                    try {
                        // final Number filterNumber = convertTo(double.class, number);
                        return o -> dop.compare(o.doubleValue(), number.doubleValue());
                    } catch (ClassCastException e) {
                        return n -> false;
                    }

                }
            }
            return n -> false;
        }

    }

    @Nullable
    default Predicate<Number> predicate(@NotNull List<MarshableFilter> filters) {
        @Nullable Predicate<Number> predicate = null;
        {
            for (@NotNull MarshableFilter f : filters) {
                predicate = (predicate == null) ?
                        toPredicate(f.filter.trim()) :
                        predicate.and(toPredicate(f.filter.trim()));
            }
        }
        return predicate;
    }

}



