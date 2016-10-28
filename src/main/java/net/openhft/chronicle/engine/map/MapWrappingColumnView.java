package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.engine.api.column.Column;
import net.openhft.chronicle.engine.api.column.MapColumnView;
import net.openhft.chronicle.engine.api.column.Row;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.wire.AbstractMarshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Predicate;

import static net.openhft.chronicle.core.util.ObjectUtils.convertTo;

/**
 * @author Rob Austin.
 */
public class MapWrappingColumnView<K, V> implements MapColumnView {

    private final MapView<K, V> mapView;
    @Nullable
    private ArrayList<String> columnNames = null;

    public MapWrappingColumnView(RequestContext requestContext,
                                 Asset asset,
                                 MapView<K, V> mapView) {
        this.mapView = mapView;
    }

    @Override
    public void registerChangeListener(@NotNull Runnable r) {
        mapView.registerSubscriber(o -> r.run());
    }

    private Comparator<Map.Entry<K, V>> sort(@NotNull final List<MarshableOrderBy> marshableOrderBy) {

        return (o1, o2) -> {
            for (@NotNull MarshableOrderBy order : marshableOrderBy) {

                if (o1 == null && o2 == null)
                    return 0;

                if (o1 == null)
                    return -1;

                if (o2 == null)
                    return 1;

                final String column = order.column;
                int result = 0;

                if (column.equals("key")) {
                    if (o1.getKey() instanceof CharSequence)
                        result = String.CASE_INSENSITIVE_ORDER.compare((String) o1.getKey(), (String) o2.getKey());
                    else if (AbstractMarshallable.class.isAssignableFrom(mapView.keyType()))
                        throw new UnsupportedOperationException();
                    else if (Comparable.class.isAssignableFrom(mapView.keyType()))
                        result = ((Comparable) o1.getKey()).compareTo(o2.getKey());


                    if (result != 0) {
                        result *= order.isAscending ? 1 : -1;
                        return result;
                    }

                    continue;
                }


                if (column.equals("value")
                        && (!(AbstractMarshallable.class.isAssignableFrom(mapView.valueType())))) {
                    if (o1.getValue() instanceof CharSequence)
                        result = String.CASE_INSENSITIVE_ORDER.compare((String) o1.getValue(), (String)
                                o2.getValue());

                    else if (o1.getValue() instanceof Comparable)
                        result = ((Comparable) o1.getValue()).compareTo(o2.getValue());

                    if (result != 0) {
                        result *= order.isAscending ? 1 : -1;
                        return result;
                    }

                    continue;

                }

                try {
                    final Field field = o1.getValue().getClass().getDeclaredField(column);
                    field.setAccessible(true);
                    @NotNull final Comparable o1Value = (Comparable) field.get(o1.getValue());
                    @NotNull final Comparable o2Value = (Comparable) field.get(o2.getValue());

                    if ((!(o1Value instanceof Comparable)) &&
                            (!(o2Value instanceof Comparable)))
                        return 0;

                    if (!(o1Value instanceof Comparable))
                        return order.isAscending ? -1 : 1;

                    if (!(o2Value instanceof Comparable))
                        return order.isAscending ? 1 : -1;

                    return o1Value.compareTo(o2Value) * (order.isAscending ? 1 : -1);

                } catch (Exception e) {
                    Jvm.warn().on(VanillaMapView.class, e);
                }

            }

            return 0;
        };


    }

    @NotNull
    @Override
    public Iterator<Row> iterator(@NotNull final SortedFilter sortedFilter) {

        final Iterator<Map.Entry<K, V>> core = mapView.entrySet().stream()
                .filter(filter(sortedFilter.marshableFilters))
                .sorted(sort(sortedFilter.marshableOrderBy))
                .iterator();

        @NotNull final Iterator<Row> result = new Iterator<Row>() {

            @Override
            public boolean hasNext() {
                return core.hasNext();
            }

            @NotNull
            @Override
            public Row next() {
                final Map.Entry e = core.next();
                @NotNull final Row row = new Row(columns());
                if (!(AbstractMarshallable.class.isAssignableFrom(mapView.keyType())))
                    row.set("key", e.getKey());
                else
                    row.set("key", e.getKey().toString());

                if (!(AbstractMarshallable.class.isAssignableFrom(mapView.valueType())))
                    row.set("value", e.getValue());
                else {
                    @NotNull final AbstractMarshallable value = (AbstractMarshallable) e.getValue();

                    for (@NotNull final Field declaredFields : mapView.valueType().getDeclaredFields()) {
                        if (!columnNames().contains(declaredFields.getName()))
                            continue;
                        try {
                            declaredFields.setAccessible(true);
                            row.set(declaredFields.getName(), declaredFields.get(value));
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
    public boolean containsRowWithKey(@NotNull Object[] keys) {
        assert keys.length == 1;
        return mapView.containsKey((K) keys[0]);
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

        if ((AbstractMarshallable.class.isAssignableFrom(keyType()))) {
            result.add(new Column("key", true, true, "", String.class, false));
        } else {
            result.add(new Column("key", false, true, "", keyType(), true));
        }

        if (!(AbstractMarshallable.class.isAssignableFrom(valueType())))
            result.add(new Column("value", false, false, "", valueType(), true));
        else {
            //valueType.isAssignableFrom()
            for (@NotNull final Field declaredFields : valueType().getDeclaredFields()) {
                result.add(new Column(declaredFields.getName(), false, false, "",
                        declaredFields.getType(), true));
            }
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

        if (!(AbstractMarshallable.class.isAssignableFrom(keyType())))
            result.add("key");

        if (!(AbstractMarshallable.class.isAssignableFrom(valueType())))
            result.add("value");
        else {
            for (@NotNull final Field declaredFields : valueType().getDeclaredFields()) {
                result.add(declaredFields.getName());
            }
        }

        columnNames = new ArrayList<>(result);
        return columnNames;
    }

    private Class<V> valueType() {
        return mapView.valueType();
    }


    @Override
    public boolean canDeleteRows() {
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
        if (!AbstractMarshallable.class.isAssignableFrom(valueType)) {
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
            final Field field;
            try {
                field = v.getClass().getDeclaredField(e.getKey());
                field.setAccessible(true);
                field.set(v, ObjectUtils.convertTo(field.getType(), e.getValue()));
            } catch (Exception e1) {
                Jvm.warn().on(VanillaMapView.class, e1);
            }
        }

        mapView.put((K) newKey, v);
        return 1;
    }

    @Nullable
    public Predicate<Map.Entry<K, V>> filter(@NotNull List<MarshableFilter> filters) {
        return entry -> {

            if (filters.isEmpty())
                return true;

            try {

                for (@NotNull MarshableFilter f : filters) {

                    Object item;

                    if ("key".equals(f.columnName)) {
                        item = entry.getKey();
                    } else if (!(AbstractMarshallable.class.isAssignableFrom(mapView.valueType())) &&
                            "value".equals(f.columnName)) {
                        item = entry.getValue();

                    } else if (AbstractMarshallable.class.isAssignableFrom(mapView.valueType())) {
                        try {
                            final Class valueClass = entry.getValue().getClass();


                            final Field field = valueClass.getDeclaredField(f.columnName);
                            field.setAccessible(true);
                            final Object o = field.get(entry.getValue());

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

    private boolean toRange(@NotNull Number o, @NotNull String trimmed) {
        if (trimmed.startsWith(">") || trimmed.startsWith("<")) {

            @NotNull final String number = trimmed.substring(1, trimmed.length()).trim();

            final Number filterNumber;
            try {
                filterNumber = convertTo(o.getClass(), number);
            } catch (ClassCastException e) {
                return false;
            }

            boolean result;
            if (trimmed.startsWith(">"))
                result = o.doubleValue() > filterNumber.doubleValue();
            else if (trimmed.startsWith("<"))
                result = o.doubleValue() < filterNumber.doubleValue();
            else
                throw new UnsupportedOperationException();
            return result;

        } else {
            final Object filterNumber = convertTo(o.getClass(), trimmed);
            return o.equals(filterNumber);
        }
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