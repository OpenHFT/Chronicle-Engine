package net.openhft.chronicle.engine.query;

import net.openhft.chronicle.core.util.SerializableFunction;
import net.openhft.chronicle.core.util.SerializablePredicate;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Rob Austin.
 */
public class Filter<E> implements Marshallable, Iterable<Operation> {

    private final List<Operation> pipeline = new ArrayList<>();

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) throws IllegalStateException {
        wireIn.read(() -> "pipeline").object(List.class);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        wireOut.write(() -> "pipeline").object(pipeline);
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

}
