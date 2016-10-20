/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
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
    private final ThreadLocal<Function<Class, Marshallable>> objectCacheThreadLocal;
    private final ThreadLocal<IndexedValue<V>> indexedValue = ThreadLocal.withInitial(IndexedValue::new);
    private final TypeToString typeToString;
    private volatile long lastIndexRead = 0;
    private long lastSecond = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    private long messagesReadPerSecond = 0;
    private ThreadLocal<List<Marshallable>> indexedValueList = ThreadLocal.withInitial(ArrayList::new);

    public VanillaIndexQueueView(@NotNull RequestContext context,
                                 @NotNull Asset asset,
                                 @NotNull QueueView<?, V> queueView) {

        valueToKey = asset.findView(ValueToKey.class);

        final EventLoop eventLoop = asset.acquireView(EventLoop.class);
        final ChronicleQueueView chronicleQueueView = (ChronicleQueueView) queueView;

        chronicleQueue = chronicleQueueView.chronicleQueue();
        final ExcerptTailer tailer = chronicleQueue.createTailer();

        typeToString = asset.root().findView(TypeToString.class);

        // use a function factory so each thread has a thread local function.
        objectCacheThreadLocal = ThreadLocal.withInitial(
                () -> asset.root().acquireView(ObjectCacheFactory.class).get());

        eventLoop.addHandler(() -> {

            long currentSecond = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());

            if (currentSecond >= lastSecond + 10) {
                lastSecond = currentSecond;
                LOG.info("messages read per second=" + messagesReadPerSecond / 10);
                messagesReadPerSecond = 0;
            }

            if (isClosed.get())
                throw new InvalidEventHandlerException();

            try (DocumentContext dc = tailer.readingDocument()) {

                if (!dc.isPresent())
                    return false;
                long start = dc.wire().bytes().readPosition();

                try {
                    while (dc.wire().bytes().readRemaining() > 0) {
                        final StringBuilder sb = Wires.acquireStringBuilder();
                        final ValueIn read = dc.wire().read(sb);

                        final V v = (V) VanillaObjectCacheFactory.INSTANCE.get()
                                .apply(typeToString.toType(sb));
                        read.marshallable(v);

                        final Object k = valueToKey.apply(v);
                        messagesReadPerSecond++;

                        final String event = sb.toString();
                        synchronized (lock) {
                            multiMap.computeIfAbsent(event, e -> new ConcurrentHashMap<>())
                                    .put(k, new IndexedValue<>(v, dc.index()));
                            lastIndexRead = dc.index();
                        }
                    }

                } catch (RuntimeException e) {
                    Jvm.warn().on(getClass(), Wires.fromSizePrefixedBlobs(dc.wire().bytes(), start - 4), e);
                }
            }

            return true;
        });
    }

    /**
     * consumers wire on the NIO socket thread
     *
     * @param sub               called when ever there is a subscription event that passes the
     *                          predicate defined by {@code vanillaIndexQuery}
     * @param vanillaIndexQuery the predicate of the subscription
     */
    public void registerSubscriber(@NotNull ConsumingSubscriber<IndexedValue<V>> sub,
                                   @NotNull IndexQuery<V> vanillaIndexQuery) {

        final AtomicBoolean isClosed = new AtomicBoolean();
        activeSubscriptions.put(sub, isClosed);

        final long fromIndex = vanillaIndexQuery.fromIndex() == 0 ? lastIndexRead : vanillaIndexQuery.fromIndex();
        final String eventName = vanillaIndexQuery.eventName();
        final Predicate<V> filter = vanillaIndexQuery.filter();

        // don't set iterator if the 'fromIndex' has not caught up.

        final Iterator<IndexedValue<V>> iterator =
                multiMap.computeIfAbsent(eventName, k -> new ConcurrentHashMap<>())
                        .values().stream()
                        .filter(i -> i.index() < fromIndex && filter.test(i.v()))
                        .iterator();

        final ExcerptTailer tailer = chronicleQueue.createTailer();

        try {
            if (fromIndex != 0)
                if (!tailer.moveToIndex(fromIndex))
                    throw new IllegalStateException("Failed to move to index " + Long.toHexString(fromIndex));
            final Supplier<List<Marshallable>> supplier = excerptConsumer(vanillaIndexQuery,
                    tailer, iterator, fromIndex);
            sub.addSupplier(supplier);

        } catch (RuntimeException e) {
            //tailer.close();
            sub.onEndOfSubscription();
            Jvm.warn().on(getClass(), "Error registering subscription", e);
        }
    }

    @NotNull
    private Supplier<List<Marshallable>> excerptConsumer(@NotNull IndexQuery<V> vanillaIndexQuery,
                                                         @NotNull ExcerptTailer tailer,
                                                         @NotNull Iterator<IndexedValue<V>> iterator,
                                                         final long fromIndex) {
        return () -> VanillaIndexQueueView.this.value(vanillaIndexQuery, tailer, iterator, fromIndex);
    }

    @Nullable
    private List<Marshallable> value(@NotNull IndexQuery<V> vanillaIndexQuery,
                                     @NotNull ExcerptTailer tailer,
                                     @NotNull Iterator<IndexedValue<V>> iterator,
                                     final long from) {
        List<Marshallable> indexedValues = indexedValueList.get();
        indexedValues.clear();

        if (iterator.hasNext()) {
            IndexedValue<V> indexedValue = iterator.next();
            indexedValue.timePublished(System.currentTimeMillis());
            indexedValue.maxIndex(lastIndexRead);
            indexedValues.add(indexedValue);
            return indexedValues;
        }

        final String eventName = vanillaIndexQuery.eventName();
        final Predicate<V> filter = vanillaIndexQuery.filter();

        if (isClosed.get())
            throw Jvm.rethrow(new InvalidEventHandlerException("shutdown"));

        try (DocumentContext dc = tailer.readingDocument()) {

            if (!dc.isPresent())
                return null;
            System.out.println(Wires.fromSizePrefixedBlobs(dc));

            if (LOG.isDebugEnabled())
                Jvm.debug().on(getClass(), "processing the following message=" + Wires.fromSizePrefixedBlobs(dc));

            // we may have just been restated and have not yet caught up
            if (from > dc.index())
                return null;

            final StringBuilder sb = Wires.acquireStringBuilder();
            while (dc.wire().bytes().readRemaining() > 0) {
                final ValueIn valueIn = dc.wire().read(sb);
                if (!eventName.contentEquals(sb)) {
                    valueIn.skipValue();
                    continue;
                }

                // allows object re-use when using marshallable
                final Function<Class, Marshallable> objectCache = objectCacheThreadLocal.get();

                final V v = (V) VanillaObjectCacheFactory.INSTANCE.get()
                        .apply(typeToString.toType(sb));
                valueIn.marshallable(v);

                if (!filter.test(v))
                    continue;

                final IndexedValue<V> indexedValue = this.indexedValue.get();
                long index = dc.index();
                indexedValue.index(index);
                indexedValue.v(v);
                indexedValue.timePublished(System.currentTimeMillis());
                indexedValue.maxIndex(lastIndexRead);
                indexedValues.add(indexedValue);
            }
        }
        return indexedValues;
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

