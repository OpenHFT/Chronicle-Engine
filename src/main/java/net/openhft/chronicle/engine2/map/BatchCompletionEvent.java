package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.map.MapEventListener;
import net.openhft.chronicle.engine2.api.map.MapReplicationEvent;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * End of a batch of bootstrap messages.
 * <p>
 * Created by peter on 22/05/15.
 */
public class BatchCompletionEvent<K, V> implements MapReplicationEvent<K, V> {
    private final long dataUpToTimeStampMS;

    private BatchCompletionEvent(long dataUpToTimeStampMS) {
        this.dataUpToTimeStampMS = dataUpToTimeStampMS;
    }

    public static <K, V> BatchCompletionEvent<K, V> of(long dataUpToTimeStampMS) {
        return new BatchCompletionEvent<>(dataUpToTimeStampMS);
    }

    @Override
    public <K2, V2> MapReplicationEvent<K2, V2> translate(Function<K, K2> keyFunction, Function<V, V2> valueFunction) {
        return (MapReplicationEvent<K2, V2>) this;
    }

    public K key() {
        return null;
    }

    @Override
    public V oldValue() {
        return null;
    }

    public V value() {
        return null;
    }

    @Override
    public boolean isDeleted() {
        return false;
    }

    @Override
    public int identifier() {
        return 0;
    }

    @Override
    public long timeStampMS() {
        return 0;
    }

    @Override
    public long dataUpToTimeStampMS() {
        return dataUpToTimeStampMS;
    }

    @Override
    public void apply(MapEventListener<K, V> listener) {

    }

    @Override
    public int hashCode() {
        return Objects.hash("batch-completion", dataUpToTimeStampMS);
    }

    @Override
    public boolean equals(Object obj) {
        return Optional.ofNullable(obj)
                .filter(o -> o instanceof BatchCompletionEvent)
                .map(o -> (BatchCompletionEvent<K, V>) o)
                .filter(e -> dataUpToTimeStampMS == e.dataUpToTimeStampMS)
                .isPresent();
    }

    @Override
    public String toString() {
        return "BootstrapEvent{" +
                "dataUpToTimeStampMS=" + dataUpToTimeStampMS +
                '}';
    }
}
