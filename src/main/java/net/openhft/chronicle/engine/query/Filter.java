package net.openhft.chronicle.engine.query;

import net.openhft.chronicle.core.util.SerializableFunction;
import net.openhft.chronicle.core.util.SerializablePredicate;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.query.Query;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * @author Rob Austin.
 */
public class Filter<E> implements Marshallable, Iterable<Operation> {

    public static Filter EMPTY = new Filter();

    List<Operation> pipeline = new ArrayList<>();

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) throws IllegalStateException {
        if (pipeline == null)
            pipeline = new ArrayList<>();
        else
            pipeline.clear();
        wireIn.read(
                () -> "pipeline").sequence(s -> {
            while (s.hasNextSequenceItem())
                pipeline.add((Operation) s.object(Object.class));
        });
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        wireOut.write(() -> "pipeline")
                .sequence(
                        w -> pipeline.forEach(w::object));
    }

    @Override
    public Iterator<Operation> iterator() {
        return pipeline.iterator();
    }

    void add(SerializablePredicate<? super E> predicate, final Operation.OperationType filter) {
        pipeline.add(new Operation(filter, predicate));
    }

    void add(SerializableFunction<? super E, ?> mapper, final Operation.OperationType map) {
        pipeline.add(new Operation(map, mapper));
    }

    <R> void add(Class<R> rClass, final Operation.OperationType project) {
        pipeline.add(new Operation(project, rClass));
    }

    @Override
    public String toString() {
        return "Filter{" +
                "pipeline=" + pipeline +
                '}';
    }

    public void addFilter(SerializablePredicate<? super E> predicate) {
        pipeline.add(new Operation(Operation.OperationType.FILTER, predicate));
    }

    public <R> void addMap(SerializableFunction<? super E, ? extends R> mapper) {
        pipeline.add(new Operation(Operation.OperationType.MAP, mapper));
    }

    public void addProject(Class rClass) {
        pipeline.add(new Operation(Operation.OperationType.PROJECT, rClass));
    }

    public <R> void addFlatMap(SerializableFunction<? super E, ? extends Query<? extends R>> mapper) {
        pipeline.add(new Operation(Operation.OperationType.FLAT_MAP, mapper));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Filter)) return false;

        Filter<?> filter = (Filter<?>) o;

        return !(pipeline != null ? !pipeline.equals(filter.pipeline) : filter.pipeline != null);

    }

    @Override
    public int hashCode() {
        return pipeline != null ? pipeline.hashCode() : 0;
    }

    /**
     * filters subscription on based on {@code net.openhft.chronicle.engine.query.Filter}
     */
    public static class FilteredSubscriber<E> implements Subscriber<E> {

        private final Subscriber<E> subscriber;
        private final Filter<E> filter;

        public FilteredSubscriber(@NotNull Filter<E> filter,
                                  @NotNull Subscriber<E> subscriber) {
            this.filter = filter;
            this.subscriber = subscriber;
        }

        @Override
        public void onMessage(@NotNull E message) throws InvalidSubscriberException {

            for (Operation o : filter) {
                switch (o.op()) {
                    case FILTER:
                        final Predicate<E> serializable = o.wrapped();
                        if (!serializable.test(message))
                            return;
                        break;

                    case MAP:
                        final Function<Object, E> function = o.wrapped();
                        message = function.apply(message);
                        break;

                    case FLAT_MAP:
                        final Function<Object, Stream<E>> func = o.wrapped();
                        func.apply(message).forEach(e -> {
                            try {
                                FilteredSubscriber.this.onMessage(e);
                            } catch (InvalidSubscriberException e1) {
                                e1.printStackTrace();
                            }
                        });
                        break;

                    case PROJECT:
                        throw new UnsupportedOperationException("todo");
                }

            }

            subscriber.onMessage(message);
        }

        @Override
        public void onEndOfSubscription() {
            subscriber.onEndOfSubscription();
        }
    }
}
