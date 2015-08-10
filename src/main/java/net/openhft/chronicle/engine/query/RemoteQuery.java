package net.openhft.chronicle.engine.query;

import net.openhft.chronicle.core.util.SerializableFunction;
import net.openhft.chronicle.core.util.SerializablePredicate;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.query.Query;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static net.openhft.chronicle.engine.query.Operation.OperationType.*;

/**
 * @author Rob Austin.
 */
public class RemoteQuery<E> implements Query<E> {

    private final MapView<E, ?> mapView;
    private final Filter<E> filter = new Filter<>();

    public RemoteQuery(@NotNull MapView<E, ?> mapView) {
        this.mapView = mapView;
    }

    @Override
    public Query<E> filter(SerializablePredicate<? super E> predicate) {
        filter.add(predicate, FILTER);
        return this;
    }

    @Override
    public <R> Query<R> map(SerializableFunction<? super E, ? extends R> mapper) {
        filter.add(mapper, MAP);
        return (Query<R>) this;
    }

    @Override
    public <R> Query<R> project(Class<R> rClass) {
        filter.add(rClass, PROJECT);
        return (Query<R>) this;
    }

    @Override
    public <R> Query<R> flatMap(SerializableFunction<? super E, ? extends Query<? extends R>> mapper) {
        filter.add(mapper, FLAT_MAP);
        return (Query<R>) this;
    }

    @Override
    public Stream<E> stream() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void subscribe(Consumer<? super E> action) {
        mapView.registerKeySubscriber(action::accept);
    }

    @Override
    public void forEach(Consumer<? super E> action) {
        mapView.registerKeySubscriber(action::accept, filter, true);
    }

    @Override
    public <R, A> R collect(Collector<? super E, A, R> collector) {
        final A container = collector.supplier().get();
        forEach(o -> collector.accumulator().accept(container, o));
        return collector.finisher().apply(container);
    }

}
