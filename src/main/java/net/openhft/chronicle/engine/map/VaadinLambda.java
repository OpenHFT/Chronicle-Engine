package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.util.SerializableBiFunction;
import net.openhft.chronicle.engine.api.column.ColumnView;
import net.openhft.chronicle.engine.api.column.ColumnView.Query;
import net.openhft.chronicle.engine.api.map.MapView;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.Map;

/**
 * @author Rob Austin.
 */
public class VaadinLambda<K, V> {


    @NotNull
    public static <K, V>
    SerializableBiFunction<MapView<K, V>, ColumnView.Query<K>, Iterator<Map.Entry<K, V>>>
    iteratorFunction() {
        return (MapView<K, V> kvMapView, Query<K> q) -> {

            Iterator<Map.Entry<K, V>> result = (Iterator) kvMapView.entrySet().stream()
                    .filter(q::filter)
                    .sorted(q.sorted())
                    .iterator();
            long x = 0;
            while (x++ < q.fromIndex && result.hasNext()) {
                result.next();
            }

            return result;
        };


    }


    @NotNull
    public static <K, V>
    SerializableBiFunction<MapView<K, V>, Query<K>, Long> countFunction() {
        return (MapView<K, V> mapView, Query<K> q) ->
                mapView.entrySet().stream()
                        .filter(q::filter)
                        .count();
    }


}
