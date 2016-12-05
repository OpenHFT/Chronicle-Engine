package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.engine.api.column.ClosableIterator;
import net.openhft.chronicle.engine.api.column.Column;
import net.openhft.chronicle.engine.api.column.MapColumnView;
import net.openhft.chronicle.engine.api.column.Row;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.wire.FieldInfo;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Predicate;

import static net.openhft.chronicle.core.util.ObjectUtils.convertTo;
import static net.openhft.chronicle.engine.api.column.ColumnViewInternal.DOp.toRange;

/**
 * @author Rob Austin.
 */
public class MapWrappingColumnView<K, V> implements MapColumnView {

    private final RequestContext requestContext;
    private final Asset asset;
    private final MapView<K, V> mapView;
    private final boolean valueMarshallable;
    private final boolean valueMap;
    @Nullable
    private ArrayList<String> columnNames = null;

    public MapWrappingColumnView(RequestContext requestContext,
                                 Asset asset,
                                 MapView<K, V> mapView) {
        this.requestContext = requestContext;
        this.asset = asset;
        this.mapView = mapView;
        valueMarshallable = Marshallable.class.isAssignableFrom(mapView.valueType());
        valueMap = Map.class.isAssignableFrom(mapView.valueType());
    }

    @Override
    public void registerChangeListener(@NotNull Runnable r) {
        mapView.registerSubscriber(o -> r.run());
    }

    private Comparator<Map.Entry<K, V>> sort(@NotNull final List<MarshableOrderBy> marshableOrderBy) {

        return (o1, o2) -> {
            if (o1 == null)
                return o2 == null ? 0 : -1;

            if (o2 == null)
                return 1;

            for (@NotNull MarshableOrderBy order : marshableOrderBy) {

                final String column = order.column;
                int result = 0;

                Object c1;
                Object c2;

                if (column.equals("key")) {
                    c1 = o1.getKey();
                    c2 = o2.getKey();

                } else if (valueMap) {
                    c1 = ((Map) o1).get(column);
                    c2 = ((Map) o2).get(column);

                } else if (valueMarshallable) {
                    try {
                        FieldInfo field = Wires.fieldInfo(o1.getValue().getClass(), column);

                        c1 = field.get(o1.getValue());
                        c2 = field.get(o2.getValue());

                    } catch (Exception e) {
                        Jvm.warn().on(MapWrappingColumnView.class, e);
                        // skip the field.
                        continue;
                    }

                } else if (column.equals("value")) {
                    c1 = o1.getValue();
                    c2 = o2.getValue();

                } else {
                    // no such column.
                    continue;
                }

                if (c1.getClass() == c2.getClass() && c1 instanceof Comparable && !(c1 instanceof CharSequence)) {
                    result = ((Comparable) c1).compareTo(c2);
                } else {
                    result = String.CASE_INSENSITIVE_ORDER.compare(c1.toString(), c2.toString());
                }

                if (result != 0)
                    return order.isAscending ? -result : result;
            }

            return 0;
        };
    }

    @NotNull
    @Override
    public ClosableIterator<Row> iterator(@NotNull final SortedFilter sortedFilter) {

        final Iterator<Map.Entry<K, V>> core = mapView.entrySet().stream()
                .filter(filter(sortedFilter.marshableFilters))
                .sorted(sort(sortedFilter.marshableOrderBy))
                .iterator();

        @NotNull final ClosableIterator<Row> result = new ClosableIterator<Row>() {

            @Override
            public void close() {
                // do nothing
            }

            @Override
            public boolean hasNext() {
                return core.hasNext();
            }

            @NotNull
            @Override
            public Row next() {
                final Map.Entry e = core.next();
                @NotNull final Row row = new Row(columns());
                if ((Marshallable.class.isAssignableFrom(mapView.keyType()))) {
                    row.set("key", e.getKey().toString());
                } else {
                    row.set("key", e.getKey());
                }

                final List<FieldInfo> fieldInfos = Wires.fieldInfos(mapView.valueType());
                if (fieldInfos.isEmpty()) {
                    row.set("value", e.getValue());

                } else {
                    @NotNull final Marshallable value = (Marshallable) e.getValue();
                    for (@NotNull final FieldInfo info : fieldInfos) {
//                        System.out.println(info);
                        if (!columnNames().contains(info.name()))
                            continue;

                        try {
                            final Object newValue = info.get(value);
//                            System.out.println("\t"+newValue);
                            row.set(info.name(), newValue);

                        } catch (Exception e1) {
                            Jvm.warn().on(VanillaMapView.class, e1);
                        }
                    }
                }

                return row;
            }
        };

        long x = 0;
        while (x++ < sortedFilter.fromIndex && result.hasNext()) {
            result.next();
        }

        return result;
    }

    @Override
    public boolean containsRowWithKey(@NotNull List keys) {
        assert keys.size() == 1;
        return mapView.containsKey((K) keys.get(0));
    }

    @Nullable
    @Override
    public ObjectSubscription objectSubscription() {
        return mapView.asset().getView(ObjectSubscription.class);
    }

    @NotNull
    @Override
    public List<Column> columns() {


        @NotNull List<Column> result = new ArrayList<>();

        if ((Marshallable.class.isAssignableFrom(keyType()))) {
            result.add(new Column("key", true, true, "", String.class, false));
        } else {
            result.add(new Column("key", true, true, "", keyType(), true));
        }

        boolean isReadOnly = requestContext.toUri().startsWith("/proc");

        if ((Marshallable.class.isAssignableFrom(valueType()))) {
            //valueType.isAssignableFrom()
            for (@NotNull final FieldInfo info : Wires.fieldInfos(valueType())) {
                result.add(new Column(info.name(), isReadOnly, false, "", info.type(), true));
            }
        } else {
            result.add(new Column("value", isReadOnly, false, "", valueType(), true));
        }

        return result;
    }

    private Class<?> keyType() {
        return mapView.keyType();
    }

    @Nullable
    private ArrayList<String> columnNames() {

        if (columnNames != null)
            return columnNames;

        @NotNull LinkedHashSet<String> result = new LinkedHashSet<>();

        result.add("key");

        if (Marshallable.class.isAssignableFrom(valueType())) {
            for (@NotNull final FieldInfo fi : Wires.fieldInfos(valueType())) {
                result.add(fi.name());
            }
        } else {
            result.add("value");
        }

        columnNames = new ArrayList<>(result);
        return columnNames;
    }

    private Class<V> valueType() {
        return mapView.valueType();
    }

    @Override
    public boolean canDeleteRows() {
        if (requestContext.toUri().startsWith("/proc"))
            return false;
        return true;
    }


    @Override
    public int changedRow(@NotNull Map<String, Object> row, @NotNull Map<String, Object> oldRow) {

        assert row.isEmpty() && oldRow.isEmpty() : "both rows can not be empty";

        if (row.isEmpty()) {
            @NotNull final K k = (K) oldRow.get("key");
            if (k == null)
                throw new IllegalStateException("key not found");
            return mapView.remove(k) == null ? 0 : 1;
        }

        final Object oldKey = oldRow.get("key");
        final Object newKey = row.get("key");

        if (oldKey != null && !oldKey.equals(newKey))
            mapView.remove((K) oldKey);

        final Class<V> valueType = mapView.valueType();
        if (!Marshallable.class.isAssignableFrom(valueType)) {
            if (!row.containsKey("value"))
                throw new IllegalStateException("value not found");
            assert row.size() == 2;
            assert oldRow.size() == 2;
            mapView.put((K) newKey, (V) row.get("value"));
            return 1;
        }

        final V v = ObjectUtils.newInstance(valueType);

        for (@NotNull Map.Entry<String, Object> e : row.entrySet()) {
            if ("key".equals(e.getKey()))
                continue;
            FieldInfo fi = Wires.fieldInfo(valueType, e.getKey());
            fi.set(v, e.getValue());
        }

        mapView.put((K) newKey, v);
        return 1;
    }

    @Nullable
    private Predicate<Map.Entry<K, V>> filter(@NotNull List<MarshableFilter> filters) {
        return entry -> {

            if (filters.isEmpty())
                return true;

            try {

                for (@NotNull MarshableFilter f : filters) {

                    Object item;

                    if ("key".equals(f.columnName)) {
                        item = entry.getKey();
                    } else if (!(Marshallable.class.isAssignableFrom(mapView.valueType())) &&
                            "value".equals(f.columnName)) {
                        item = entry.getValue();

                    } else if (Marshallable.class.isAssignableFrom(mapView.valueType())) {
                        try {
                            final Class valueClass = entry.getValue().getClass();
                            final FieldInfo info = Wires.fieldInfo(valueClass, f.columnName);
                            final Object o = info.get(entry.getValue());

                            if (o == null)
                                return false;
                            if (o instanceof Number) {
                                if (toRange((Number) o, f.filter.trim()))
                                    continue;
                                return false;
                            }
                            item = o;

                        } catch (Exception e) {
                            return false;
                        }

                    } else {
                        throw new UnsupportedOperationException();
                    }

                    if (item instanceof CharSequence) {
                        if (!item.toString().toLowerCase().contains(f.filter.toLowerCase()))
                            return false;
                    } else if (item instanceof Number) {
                        if (!toRange((Number) item, f.filter.trim()))
                            return false;
                    } else {
                        if (!item.equals(convertTo(item.getClass(), f.filter.trim())))
                            return false;
                    }

                }

                return true;

            } catch (NumberFormatException e) {
                return false;
            }
        };
    }



    /**
     * @param sortedFilter if {@code sortedFilter} == null or empty all the total number of rows is
     *                     returned
     * @return the number of rows the matches this query
     */
    @Override
    public int rowCount(@Nullable List<MarshableFilter> sortedFilter) {
        if (sortedFilter == null || sortedFilter.isEmpty())
            return (int) mapView.longSize();

        return (int) mapView.entrySet().stream()
                .filter(filter(sortedFilter))
                .count();
    }

}