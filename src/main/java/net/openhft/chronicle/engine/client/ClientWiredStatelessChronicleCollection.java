package net.openhft.chronicle.engine.client;

import net.openhft.chronicle.map.MapStatelessClient;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.collection.CollectionWireHandler.SetEventId;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Function;

import static net.openhft.chronicle.wire.CoreFields.reply;
import static net.openhft.chronicle.wire.collection.CollectionWireHandler.SetEventId.*;


public class ClientWiredStatelessChronicleCollection<U> extends MapStatelessClient<SetEventId>
        implements Collection<U> {

    private final Function<ValueIn, U> consumer;

    public ClientWiredStatelessChronicleCollection(@NotNull final String channelName,
                                                   @NotNull final ClientWiredStatelessTcpConnectionHub hub,
                                                   final long cid,
                                                   @NotNull final Function<ValueIn, U> wireToSet,
                                                   @NotNull final String type) {

        super(channelName, hub, type, cid);
        this.consumer = wireToSet;
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

    @NotNull
    @Override
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
    private Collection<U> segmentSet(int segment) {

        final Collection<U> e = new ArrayList<U>();

        proxyReturnWireConsumerInOut(iterator, reply, valueOut -> valueOut.uint16(segment),
                read -> {

                    while (read.hasNextSequenceItem()) {
                        read.sequence(v -> e.add(consumer.apply(read)));
                    }

                    return e;
                });


        return e;
    }

    @NotNull
    @Override
    public Object[] toArray() {
        return asCollection().toArray();
    }

    @NotNull
    private Collection<U> asCollection() {

        final Collection<U> e = new ArrayList<U>();
        final int numberOfSegments = proxyReturnUint16(SetEventId.numberOfSegments);

        for (long j = 0; j < numberOfSegments; j++) {

            final long i = j;
            proxyReturnWireConsumerInOut(iterator, reply, valueOut -> valueOut.uint16(i),

                    read -> {
                        while (read.hasNextSequenceItem()) {
                            read.sequence(v -> e.add(consumer.apply(read)));
                        }

                        return e;
                    });
        }
        return e;
    }

    @NotNull
    @Override
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