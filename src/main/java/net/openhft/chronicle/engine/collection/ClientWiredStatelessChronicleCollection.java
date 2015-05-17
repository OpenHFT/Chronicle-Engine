package net.openhft.chronicle.engine.collection;

import net.openhft.chronicle.engine.client.ClientWiredStatelessTcpConnectionHub;
import net.openhft.chronicle.engine.collection.CollectionWireHandler.SetEventId;
import net.openhft.chronicle.map.MapStatelessClient;
import net.openhft.chronicle.wire.ValueIn;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Supplier;

import static net.openhft.chronicle.engine.collection.CollectionWireHandler.SetEventId.*;
import static net.openhft.chronicle.wire.CoreFields.reply;


public class ClientWiredStatelessChronicleCollection<U, E extends Collection<U>> extends
        MapStatelessClient<SetEventId> implements Collection<U> {

    private final Function<ValueIn, U> consumer;
    private final Supplier<E> factory;

    public ClientWiredStatelessChronicleCollection(@NotNull final String channelName,
                                                   @NotNull final ClientWiredStatelessTcpConnectionHub hub,
                                                   final long cid,
                                                   @NotNull final Function<ValueIn, U> wireToSet,
                                                   @NotNull final String type,
                                                   @NotNull Supplier<E> factory) {
        super(channelName, hub, type, cid);
        this.consumer = wireToSet;
        this.factory = factory;
    }

    @Override
    public int size() {
        return proxyReturnInt(size);
    }

    @Override
    public boolean isEmpty() {
        return proxyReturnBoolean(isEmpty);
    }

    @Override
    public boolean contains(Object o) {
        return proxyReturnBooleanWithArgs(contains, o);
    }

    @Override
    @NotNull
    public Iterator<U> iterator() {

        // todo improve numberOfSegments
        final int numberOfSegments = proxyReturnUint16(SetEventId.numberOfSegments);

        // todo iterate the segments for the moment we just assume one segment
        return segmentSet(1).iterator();

    }


    /**
     * gets the iterator for a given segment
     *
     * @param segment the maps segment number
     * @return and iterator for the {@code segment}
     */
    @NotNull
    private E segmentSet(int segment) {

        final E e = factory.get();

        return proxyReturnWireConsumerInOut(iterator, reply, valueOut -> valueOut.uint16(segment),
                read -> {
                    read.sequence(s -> {
                        do {
                            e.add(consumer.apply(read));
                        } while (read.hasNextSequenceItem());
                    });
                    return e;
                });
    }

    @Override
    @NotNull
    public Object[] toArray() {
        return asCollection().toArray();
    }


    @NotNull
    private E asCollection() {

        final E e = factory.get();
        final int numberOfSegments = proxyReturnUint16(SetEventId.numberOfSegments);

        for (long j = 0; j < numberOfSegments; j++) {

            final long i = j;
            proxyReturnWireConsumerInOut(iterator, reply, valueOut -> valueOut.uint16(i),
                    read -> read.sequence(r -> {
                        while (r.hasNextSequenceItem()) {
                            e.add(consumer.apply(r));
                        }
                    }));
        }
        return e;
    }

    @Override
    @NotNull
    public <T> T[] toArray(T[] array) {
        return asCollection().toArray(array);
    }

    @Override
    public boolean add(U u) {
        return proxyReturnBoolean(add);
    }

    @Override
    public boolean remove(Object o) {
        return proxyReturnBooleanWithArgs(remove, o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return proxyReturnBooleanWithSequence(containsAll, c);
    }

    @Override
    public boolean addAll(Collection<? extends U> c) {
        return proxyReturnBooleanWithSequence(addAll, c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return proxyReturnBooleanWithSequence(retainAll, c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return proxyReturnBooleanWithSequence(removeAll, c);
    }

    @Override
    public void clear() {
        proxyReturnVoid(clear);
    }

}