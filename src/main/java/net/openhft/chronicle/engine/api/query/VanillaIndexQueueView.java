package net.openhft.chronicle.engine.api.query;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.engine.api.pubsub.ConsumingSubscriber;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.tree.ChronicleQueueView;
import net.openhft.chronicle.engine.tree.QueueView;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * @author Rob Austin.
 */
public class VanillaIndexQueueView<V extends Marshallable>
        implements IndexQueueView<ConsumingSubscriber<IndexedValue<V>>, V> {

    private static final Logger LOG = LoggerFactory.getLogger(VanillaIndexQueueView.class);

    private final Function<V, ?> valueToKey;
    private final ChronicleQueue chronicleQueue;
    private final Map<String, Map<Object, IndexedValue<V>>> multiMap = new ConcurrentHashMap<>();
    private final Map<Subscriber<IndexedValue<V>>, AtomicBoolean> activeSubscriptions
            = new ConcurrentHashMap<>();
    private final AtomicBoolean isClosed = new AtomicBoolean();

    private final Object lock = new Object();
    private final ThreadLocal<Function<Class, ReadMarshallable>> objectCacheThreadLocal;
    private volatile long lastIndexRead = 0;
    private long currentSecond = 0;
    private long messagesReadPerSecond = 0;

    public VanillaIndexQueueView(@NotNull RequestContext context,
                                 @NotNull Asset asset,
                                 @NotNull QueueView<?, V> queueView) {

        valueToKey = asset.acquireView(ValueToKey.class);

        final EventLoop eventLoop = asset.acquireView(EventLoop.class);
        final ChronicleQueueView chronicleQueueView = (ChronicleQueueView) queueView;

        chronicleQueue = chronicleQueueView.chronicleQueue();
        final ExcerptTailer tailer = chronicleQueue.createTailer();

        // use a function factory so each thread has a thread local function.
        objectCacheThreadLocal = ThreadLocal.withInitial(
                () -> asset.root().acquireView(ObjectCacheFactory.class).get());

        eventLoop.addHandler(() -> {

            long second = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());

            if (currentSecond != second) {
                currentSecond = second;
                System.out.println("messages read per second=" + messagesReadPerSecond);
                messagesReadPerSecond = 0;
            }


            if (isClosed.get())
                throw new InvalidEventHandlerException();

            try (DocumentContext dc = tailer.readingDocument()) {

                if (!dc.isPresent())
                    return false;

                final StringBuilder sb = Wires.acquireStringBuilder();
                final ValueIn read = dc.wire().read(sb);

                final V v = read.typedMarshallable();
                final Object k = valueToKey.apply(v);
                messagesReadPerSecond++;

                final String event = sb.toString();
                synchronized (lock) {
                    multiMap.computeIfAbsent(event, e -> new ConcurrentHashMap<>())
                            .put(k, new IndexedValue<>(v, dc.index()));
                    lastIndexRead = dc.index();
                }
            } catch (Exception e) {
                LOG.error("", e);
            }

            return true;
        });
    }

    /**
     * consumers wire on the NIO socket thread
     *
     * @param sub
     * @param vanillaIndexQuery
     * @return
     */
    public void registerSubscriber(@NotNull ConsumingSubscriber<IndexedValue<V>> sub,
                                   @NotNull IndexQuery<V> vanillaIndexQuery) {

        final AtomicBoolean isClosed = new AtomicBoolean();
        activeSubscriptions.put(sub, isClosed);

        final long fromIndex = vanillaIndexQuery.fromIndex() == 0 ? lastIndexRead : vanillaIndexQuery.fromIndex();
        final String eventName = vanillaIndexQuery.eventName();
        final Predicate<V> filter = vanillaIndexQuery.filter();

        // don't set iterator if the 'fromIndex' has not caught up.
        final Predicate<IndexedValue<V>> predicate = fromIndex > lastIndexRead ?
                i -> i.index() <= fromIndex && filter.test(i.v()) :
                i -> filter.test(i.v());

        final Iterator<IndexedValue<V>> iterator =
                multiMap.computeIfAbsent(eventName, k -> new ConcurrentHashMap<>())
                        .values().stream()
                        .filter(predicate)
                        .iterator();

        final ExcerptTailer tailer = chronicleQueue.createTailer();

        try {
            if (fromIndex != 0)
                tailer.moveToIndex(fromIndex);
            final Supplier<Marshallable> consumer = excerptConsumer(vanillaIndexQuery, tailer, iterator, fromIndex);
            sub.addValueOutConsumer(consumer);
        } catch (TimeoutException e) {
            tailer.close();
            sub.onEndOfSubscription();
            LOG.error("timeout", e);
        }

    }

    @NotNull
    private Supplier<Marshallable> excerptConsumer(@NotNull IndexQuery<V> vanillaIndexQuery,
                                                   @NotNull ExcerptTailer tailer,
                                                   @NotNull Iterator<IndexedValue<V>> iterator,
                                                   final long fromIndex) {
        return () -> value(vanillaIndexQuery, tailer, iterator, fromIndex);
    }


    private final ThreadLocal<IndexedValue<V>> indexedValue = ThreadLocal.withInitial(IndexedValue::new);

    @Nullable
    private IndexedValue<V> value(@NotNull IndexQuery<V> vanillaIndexQuery,
                                  @NotNull ExcerptTailer tailer,
                                  @NotNull Iterator<IndexedValue<V>> iterator,
                                  final long from) {

        if (iterator.hasNext())
            return iterator.next();

        final String eventName = vanillaIndexQuery.eventName();
        final Predicate<V> filter = vanillaIndexQuery.filter();

        if (isClosed.get()) {
            tailer.close();
            throw Jvm.rethrow(new InvalidEventHandlerException("shutdown"));
        }

        try (DocumentContext dc = tailer.readingDocument()) {

            if (!dc.isPresent())
                return null;

//            System.out.println(Wires.fromSizePrefixedBlobs(dc));

            // we may have just been restated and have not yet caught up
            if (from >= dc.index())
                return null;

            final StringBuilder sb = Wires.acquireStringBuilder();
            ValueIn valueIn = dc.wire().read(sb);
            if (!eventName.contentEquals(sb))
                return null;

            // allows object re-use when using marshallable
            final Function<Class, ReadMarshallable> objectCache = objectCacheThreadLocal.get();
            final V v = valueIn.typedMarshallable(objectCache);
            if (!filter.test(v))
                return null;

            final IndexedValue<V> indexedValue = this.indexedValue.get();
            indexedValue.index(dc.index());
            indexedValue.v(v);
            return indexedValue;
        }

    }

    public void unregisterSubscriber(@NotNull ConsumingSubscriber<IndexedValue<V>> listener) {
        final AtomicBoolean isClosed = activeSubscriptions.remove(listener);
        if (isClosed != null) isClosed.set(true);
    }

    @Override
    public void close() {
        isClosed.set(true);
        activeSubscriptions.values().forEach(v -> v.set(true));
        chronicleQueue.close();
    }

}

