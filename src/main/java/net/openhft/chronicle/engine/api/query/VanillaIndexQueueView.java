package net.openhft.chronicle.engine.api.query;

import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.engine.api.pubsub.ConsumingSubscriber;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.tree.ChronicleQueueView;
import net.openhft.chronicle.engine.tree.QueueView;
import net.openhft.chronicle.network.connection.VanillaWireOutPublisher.WireOutConsumer;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author Rob Austin.
 */
public class VanillaIndexQueueView<V extends Marshallable>
        implements IndexQueueView<ConsumingSubscriber<IndexedValue<V>>, V> {

    private static final Logger LOG = LoggerFactory.getLogger(VanillaIndexQueueView.class);
    private final Function<V, ?> valueToKey;
    private final EventLoop eventLoop;
    private final ChronicleQueue chronicleQueue;

    private final Map<String, Map<Object, IndexedValue<V>>> multiMap = new ConcurrentHashMap<>();
    private final Map<Subscriber<IndexedValue<V>>, AtomicBoolean> activeSubscriptions
            = new ConcurrentHashMap<>();

    private final AtomicBoolean isClosed = new AtomicBoolean();

    private long lastIndexRead = 0;
    private final Object lock = new Object();

    public VanillaIndexQueueView(@NotNull RequestContext context,
                                 @NotNull Asset asset,
                                 @NotNull QueueView<?, V> queueView) {

        valueToKey = asset.acquireView(ValueToKey.class);
        eventLoop = asset.acquireView(EventLoop.class);

        final ChronicleQueueView chronicleQueueView = (ChronicleQueueView) queueView;
        chronicleQueue = chronicleQueueView.chronicleQueue();

        final ExcerptTailer tailer = chronicleQueue.createTailer();


        eventLoop.addHandler(() -> {

            if (isClosed.get())
                throw new InvalidEventHandlerException();

            try (DocumentContext dc = tailer.readingDocument()) {

                if (!dc.isPresent())
                    return false;

                final StringBuilder sb = Wires.acquireStringBuilder();
                final ValueIn read = dc.wire().read(sb);

                final V v = read.typedMarshallable();
                final Object k = valueToKey.apply(v);

                final String event = sb.toString();
                synchronized (lock) {
                    multiMap.computeIfAbsent(event, e -> new ConcurrentHashMap<>())
                            .put(k, new IndexedValue<V>(v, dc.index()));
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
    @Nullable
    public void registerSubscriber(@NotNull ConsumingSubscriber<IndexedValue<V>> sub,
                                   @NotNull IndexQuery<V> vanillaIndexQuery) {
        // public void registerSubscriber(@NotNull ConsumingSubscriber<IndexedValue<V>> sub, @NotNull
        //      IndexQuery<V> vanillaIndexQuery) {

        final AtomicBoolean isClosed = new AtomicBoolean();
        activeSubscriptions.put(sub, isClosed);

        final long from = vanillaIndexQuery.from() == 0
                ? lastIndexRead : vanillaIndexQuery.from();

        if (from != 0) {

            final String eventName = vanillaIndexQuery.eventName();
            final Predicate<V> filter = vanillaIndexQuery.filter();

            if (from == lastIndexRead)
                multiMap.computeIfAbsent(eventName, k -> new ConcurrentHashMap<>())
                        .values().stream()
                        .filter((IndexedValue<V> i) -> filter.test(i.v()))
                        .forEach(sub);
            else
                // sends all the latest values that wont get sent via the queue
                multiMap.computeIfAbsent(eventName, k -> new ConcurrentHashMap<>())
                        .values().stream()
                        .filter((IndexedValue<V> i) -> i.index() <= from && filter.test(i.v()))
                        .forEach(sub);
        }


        final ExcerptTailer tailer = chronicleQueue.createTailer();

        try {
            if (from != 0)
                tailer.moveToIndex(from);
            WireOutConsumer consumer = excerptConsumer(vanillaIndexQuery, tailer);
            sub.wireOutConsumer(consumer);

        } catch (TimeoutException e) {
            tailer.close();
            sub.onEndOfSubscription();
            LOG.error("timeout", e);
        }

    }

    @NotNull
    private WireOutConsumer excerptConsumer(@NotNull IndexQuery<V> vanillaIndexQuery, ExcerptTailer tailer) {
        return out -> {
            final String eventName = vanillaIndexQuery.eventName();
            final Predicate<V> filter = vanillaIndexQuery.filter();

            if (isClosed.get()) {
                tailer.close();
                throw new InvalidEventHandlerException("shutdown");
            }

            try (DocumentContext dc = tailer.readingDocument()) {

                if (!dc.isPresent())
                    return;

                final StringBuilder sb = Wires.acquireStringBuilder();
                if (!eventName.contentEquals(sb))
                    return;

                V v = dc.wire().read(sb).typedMarshallable();
                if (!filter.test(v))
                    return;

                out.getValueOut().typedMarshallable(new IndexedValue<V>(v, dc.index()));
            }
        };
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
