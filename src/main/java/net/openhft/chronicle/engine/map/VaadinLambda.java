package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.util.SerializableBiFunction;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.wire.AbstractMarshallable;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * @author Rob Austin.
 */
public class VaadinLambda<K, V> {

    public enum Type {
        key, value
    }

    public static class Filter extends AbstractMarshallable {
        Type type;
        Object value;

        public Filter(Type type, Object value) {
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

    public static class Query<K, V> {
        public long fromIndex;
        public final List<MarshableOrderBy> marshableOrderBy = new ArrayList<>();
        public final List<Filter> filters = new ArrayList<>();

        public boolean filter(@NotNull Map.Entry<K, V> entry) {
            for (Filter f : filters) {

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

        public Comparator<Map.Entry<K, V>> sorted() {
            return this::compare;
        }

        private int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
            for (MarshableOrderBy order : marshableOrderBy) {

                int result = 0;
                if ("key".equals(order.column))
                    result = ((Comparable) o1.getKey()).compareTo(o2.getKey());
                else if ("value".equals(order.column))
                    result = ((Comparable) o1.getValue()).compareTo(o2.getValue());

                result *= order.isAscending ? 1 : -1;
                if (result != 0)
                    return result;

            }
            return 0;
        }
    }

    @NotNull
    public static <K, V>
    SerializableBiFunction<MapView<K, V>, Query<K, V>, Iterator<Map.Entry<K, V>>> apply(Query<K, V> query) {
        return (MapView<K, V> kvMapView, Query<K, V> q) -> {

            Iterator<Map.Entry<K, V>> result = kvMapView.entrySet().stream()
                    .filter(q::filter)
                    .sorted(q.sorted())
                    .iterator();
            long x = 0;
            while (x++ < query.fromIndex && result.hasNext()) {
                result.next();
            }

            return result;
        };
    }

}
