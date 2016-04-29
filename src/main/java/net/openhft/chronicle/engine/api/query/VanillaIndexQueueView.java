package net.openhft.chronicle.engine.api.query;

import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.tree.ChronicleQueueView;
import net.openhft.chronicle.engine.tree.QueueView;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
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
public class VanillaIndexQueueView<K extends Marshallable, V extends Marshallable>
        implements IndexQueueView<IndexedValue<V>, V> {

    private static final Logger LOG = LoggerFactory.getLogger(VanillaIndexQueueView.class);
    private final Function<V, K> valueToKey;
    private final EventLoop eventLoop;
    private final ChronicleQueue chronicleQueue;

    private final Map<String, Map<K, IndexedValue<V>>> multiMap = new ConcurrentHashMap<>();
    private final Map<Subscriber<IndexedValue<V>>, AtomicBoolean> activeSubscriptions
            = new ConcurrentHashMap<>();

    private final AtomicBoolean isClosed = new AtomicBoolean();

    private long lastIndexRead = 0;

    public VanillaIndexQueueView(@NotNull RequestContext context,
                                 @NotNull Asset asset,
                                 @NotNull QueueView<K, V> queueView) {

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
                final K k = valueToKey.apply(v);

                final String event = sb.toString();

                multiMap.computeIfAbsent(event, e -> new ConcurrentHashMap<>())
                        .put(k, new IndexedValue<V>(k, v, dc.index()));
                lastIndexRead = dc.index();
            } catch (Exception e) {
                LOG.error("", e);
            }

            return true;
        });
    }

    /**
     * bootstraps the initial subscription data from the map
     *
     * @param sub
     * @param vanillaIndexQuery
     * @param from
     */
    private void bootstrap(@NotNull Subscriber<IndexedValue<V>> sub,
                           @NotNull IndexQuery<V> vanillaIndexQuery, long from) {

        final String eventName = vanillaIndexQuery.eventName();
        final Predicate<V> filter = vanillaIndexQuery.filter();

        if (from == lastIndexRead)
            multiMap.computeIfAbsent(eventName, k -> new ConcurrentHashMap<>())
                    .values().stream()
                    .filter((IndexedValue<V>i) -> filter.test(i.v()))
                    .forEach(sub);
        else
            // sends all the latest values that wont get sent via the queue
            multiMap.computeIfAbsent(eventName, k -> new ConcurrentHashMap<>())
                    .values().stream()
                    .filter((IndexedValue<V> i) -> i.index() <= from && filter.test(i.v()))
                    .forEach(sub);
    }


    public void registerSubscriber(@NotNull Subscriber<IndexedValue<V>> sub,
                                   @NotNull IndexQuery<V> vanillaIndexQuery) {

        final AtomicBoolean isClosed = new AtomicBoolean();
        activeSubscriptions.put(sub, isClosed);

        final long from = vanillaIndexQuery.from() == 0
                ? lastIndexRead : vanillaIndexQuery.from();

        if (from != 0)
            bootstrap(sub, vanillaIndexQuery, from);

        final String eventName = vanillaIndexQuery.eventName();
        final Predicate<V> filter = vanillaIndexQuery.filter();
        final ExcerptTailer tailer = chronicleQueue.createTailer();

        try {
            if (from != 0)
                tailer.moveToIndex(from);
        } catch (TimeoutException e) {
            tailer.close();
            sub.onEndOfSubscription();
            LOG.error("timeout", e);
            return;
        }

        eventLoop.addHandler(() -> {
            if (isClosed.get()) {
                tailer.close();
                throw new InvalidEventHandlerException("shutdown");
            }

            try (DocumentContext dc = tailer.readingDocument()) {

                if (!dc.isPresent())
                    return false;

                final StringBuilder sb = Wires.acquireStringBuilder();
                V v = dc.wire().read(() -> "BookPosition").typedMarshallable();

                if (!eventName.contentEquals(sb))
                    return true;

                System.out.println("filter=" + filter.toString());
                if (!filter.test(v))
                    return true;

                try {
                    sub.onMessage(new IndexedValue<V>(valueToKey.apply(v), v, dc.index()));
                } catch (InvalidSubscriberException e) {
                    unregisterSubscriber(sub);
                }
            }

            return true;
        });

    }


    public void unregisterSubscriber(@NotNull Subscriber<IndexedValue<V>> listener) {
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
