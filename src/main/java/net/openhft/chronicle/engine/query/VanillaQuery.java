package net.openhft.chronicle.engine.query;

import net.openhft.chronicle.core.util.SerializableFunction;
import net.openhft.chronicle.core.util.SerializablePredicate;
import net.openhft.chronicle.engine.api.query.Query;

import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * Created by peter.lawrey on 12/07/2015.
 */
public class VanillaQuery<E> implements Query<E> {
    private final Stream<E> stream;

    public VanillaQuery(Stream<E> stream) {
        this.stream = stream;
    }

    @Override
    public Query<E> filter(SerializablePredicate<? super E> predicate) {
        return new VanillaQuery<>(stream.filter(predicate));
    }

    @Override
    public <R> Query<R> map(SerializableFunction<? super E, ? extends R> mapper) {
        return new VanillaQuery<>(stream.map(mapper));
    }

    @Override
    public <R> Query<R> project(Class<R> rClass) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <R> Query<R> flatMap(SerializableFunction<? super E, ? extends Query<? extends R>> mapper) {
        return new VanillaQuery<>(stream.flatMap(e -> mapper.apply(e).stream()));
    }

    @Override
    public Stream<E> stream() {
        return stream;
    }

    @Override
    public void subscribe(Consumer<? super E> action) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <R, A> R collect(Collector<? super E, A, R> collector) {
        return stream.collect(collector);
    }

    @Override
    public void forEach(Consumer<? super E> action) {
        stream.forEach(action);
    }
}
