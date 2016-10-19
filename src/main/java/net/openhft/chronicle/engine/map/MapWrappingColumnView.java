package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.engine.api.column.Column;
import net.openhft.chronicle.engine.api.column.ColumnView;
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
public class MapWrappingColumnView<K, V> implements ColumnView {


    private final MapView<K, V> mapView;
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

    private Comparator<Map.Entry<K, V>> sort(final List<MarshableOrderBy> marshableOrderBy) {

        return (o1, o2) -> {
            for (MarshableOrderBy order : marshableOrderBy) {

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
                    final Comparable o1Value = (Comparable) field.get(o1.getValue());
                    final Comparable o2Value = (Comparable) field.get(o2.getValue());

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


    @Override
    public Iterator<Row> iterator(final ColumnView.Query query) {

        final Iterator<Map.Entry<K, V>> core = mapView.entrySet().stream()
                .filter(filter(query))
                .sorted(sort(query.marshableOrderBy))
                .iterator();

        final Iterator<Row> result = new Iterator<Row>() {

            @Override
            public boolean hasNext() {
                return core.hasNext();
            }

            @Override
            public Row next() {
                final Map.Entry e = core.next();
                final Row row = new Row(columns());
                if (!(AbstractMarshallable.class.isAssignableFrom(mapView.keyType())))
                    row.set("key", e.getKey());
                else
                    throw new UnsupportedOperationException("todo");

                if (!(AbstractMarshallable.class.isAssignableFrom(mapView.valueType())))
                    row.set("value", e.getValue());
                else {
                    final AbstractMarshallable value = (AbstractMarshallable) e.getValue();

                    for (final Field declaredFields : mapView.valueType().getDeclaredFields()) {
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
        while (x++ < query.fromIndex && result.hasNext()) {
            result.next();
        }


        return result;
    }

    @Override
    public boolean containsRowWithKey(Object[] keys) {
        assert keys.length == 1;
        return mapView.containsKey((K) keys[0]);
    }

    @Override
    public ObjectSubscription objectSubscription() {
        return mapView.asset().getView(ObjectSubscription.class);
    }

    @Override
    public List<Column> columns() {
        List<Column> result = new ArrayList<>();

        if (!(AbstractMarshallable.class.isAssignableFrom(keyType())))
            result.add(new Column("key", false, true, "", keyType()));

        if (!(AbstractMarshallable.class.isAssignableFrom(valueType())))
            result.add(new Column("value", false, false, "", valueType()));
        else {
            //valueType.isAssignableFrom()
            for (final Field declaredFields : valueType().getDeclaredFields()) {
                result.add(new Column(declaredFields.getName(), false, false, "",
                        declaredFields.getType()));
            }
        }

        return result;

    }

    private Class<?> keyType() {
        return mapView.keyType();
    }

    private ArrayList<String> columnNames() {

        if (columnNames != null)
            return columnNames;

        LinkedHashSet<String> result = new LinkedHashSet<>();

        if (!(AbstractMarshallable.class.isAssignableFrom(keyType())))
            result.add("key");

        if (!(AbstractMarshallable.class.isAssignableFrom(valueType())))
            result.add("value");
        else {
            for (final Field declaredFields : valueType().getDeclaredFields()) {
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
            final K k = (K) oldRow.get("key");
            if (k == null)
                throw new IllegalStateException("key not found");
            return mapView.remove(k) == null ? 0 : 1;
        }

        Object oldKey = oldRow.get("key");
        Object newKey = row.get("key");

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

        for (Map.Entry<String, Object> e : row.entrySet()) {
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

    public Predicate<Map.Entry<K, V>> filter(@NotNull ColumnView.Query query) {
        return entry -> {

            if (query.marshableFilters.isEmpty())
                return true;

            try {

                for (MarshableFilter f : query.marshableFilters) {

                    Object item;

                    if ("key".equals(f.columnName)) {
                        item = entry.getKey();
                    } else if (!(AbstractMarshallable.class.isAssignableFrom(mapView.valueType()
                    )) &&
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

                    if (item instanceof CharSequence)
                        if (item.toString().toLowerCase().contains(f.filter.toLowerCase()))
                            continue;
                        else
                            return false;
                    else if (item instanceof Number) {
                        if (toRange((Number) item, f.filter.trim()))
                            continue;
                        else
                            return false;
                    } else {
                        if (item.equals(convertTo(item.getClass(), f.filter.trim())))
                            continue;
                        else
                            return false;
                    }

                }

                return true;

            } catch (NumberFormatException e) {
                return false;
            }
        };

    }

    private boolean toRange(Number o, String trimmed) {
        if (trimmed.startsWith(">") ||
                trimmed.startsWith("<")) {
            final String number = trimmed.substring(1, trimmed.length()).trim();

            final Object filterNumber;
            try {
                filterNumber = convertTo(o.getClass(), number);
            } catch (ClassCastException e) {
                return false;
            }

            boolean result;
            if (trimmed.startsWith(">"))
                result = o.doubleValue() > ((Number)
                        filterNumber).doubleValue();
            else if (trimmed.startsWith("<"))
                result = o.doubleValue() < ((Number)
                        filterNumber).doubleValue();
            else
                throw new UnsupportedOperationException();
            return result;

        } else {
            final Object filterNumber = convertTo(o.getClass(), trimmed);
            return o.equals(filterNumber);
        }
    }


    /**
     * @param query if {@code query} == null all the total number of rows is returned
     * @return the number of rows the matches this query
     */
    @Override
    public int rowCount(@Nullable ColumnView.Query query) {

        if (query == null)
            return (int) mapView.longSize();

        return (int) mapView.entrySet().stream()
                .filter(filter(query))
                .count();
    }
}