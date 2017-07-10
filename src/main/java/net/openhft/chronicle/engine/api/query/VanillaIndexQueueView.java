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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.pool.StringBuilderPool;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.engine.api.pubsub.ConsumingSubscriber;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.tree.ChronicleQueueView;
import net.openhft.chronicle.engine.tree.QueueView;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static net.openhft.chronicle.engine.api.query.IndexQuery.FROM_START;
import static net.openhft.chronicle.wire.Wires.*;

/**
 * @author Rob Austin.
 */
public class VanillaIndexQueueView<V extends Marshallable>
        implements IndexQueueView<ConsumingSubscriber<IndexedValue<V>>, V> {

    private static final Logger LOG = LoggerFactory.getLogger(VanillaIndexQueueView.class);
    private static final Iterator EMPTY_ITERATOR = Collections.EMPTY_LIST.iterator();


    @Nullable
    private final ChronicleQueue chronicleQueue;
    private final Map<String, ConcurrentMap<Object, IndexedValue<V>>> multiMap = new ConcurrentHashMap<>();
    private final Map<Subscriber<IndexedValue<V>>, AtomicBoolean> activeSubscriptions
            = new ConcurrentHashMap<>();
    private final AtomicBoolean isClosed = new AtomicBoolean();

    private final Object lastIndexLock = new Object();
    private final ThreadLocal<IndexedValue<V>> indexedValue = ThreadLocal.withInitial(IndexedValue::new);
    @Nullable
    private final TypeToString typeToString;
    @NotNull
    private final Asset asset;
    @NotNull
    private final StringBuilderPool eventNameDeserialiserPool = new StringBuilderPool();
    private volatile long lastIndexRead = 0;
    private long lastSecond = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    private long messagesReadPerSecond = 0;

    @NotNull
    private ConcurrentMap<Bytes, BytesStore> bytesToKey = new ConcurrentHashMap<>();

    public VanillaIndexQueueView(@NotNull RequestContext context,
                                 @NotNull Asset asset,
                                 @NotNull QueueView<?, V> queueView) {


        this.asset = asset;
        @NotNull final EventLoop eventLoop = asset.acquireView(EventLoop.class);
        @NotNull final ChronicleQueueView chronicleQueueView = (ChronicleQueueView) queueView;

        chronicleQueue = chronicleQueueView.chronicleQueue();
        @NotNull final ExcerptTailer tailer = chronicleQueue.createTailer();

        @NotNull AtomicBoolean hasMovedToStart = new AtomicBoolean();

        typeToString = asset.root().findView(TypeToString.class);

        eventLoop.addHandler(() -> {

            // the first time this is run, we move to the start of the current cycle
            if (!hasMovedToStart.get()) {
                @NotNull final RollingChronicleQueue chronicleQueue = (RollingChronicleQueue) this.chronicleQueue;
                final int cycle = chronicleQueue.cycle();
                long startOfCurrentCycle = chronicleQueue.rollCycle().toIndex(cycle, 0);
                final boolean success = tailer.moveToIndex(startOfCurrentCycle);
                hasMovedToStart.set(success);
                if (!success)
                    return false;
            }


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
                    for (; ; ) {
                        dc.wire().consumePadding();

                        if (dc.wire().bytes().readRemaining() == 0)
                            return true;

                        final StringBuilder sb = acquireStringBuilder();
                        @NotNull final ValueIn read = dc.wire().read(sb);

                        if ("history".contentEquals(sb)) {
                            read.marshallable(MessageHistory.get());
                            return true;
                        }

                        if (sb.length() == 0)
                            continue;
                        Class<? extends Marshallable> type = typeToString.toType(sb);
                        if (type == null)
                            continue;
                        @NotNull final V v = (V) VanillaObjectCacheFactory.INSTANCE.get()
                                .apply(type);
                        long readPosition = dc.wire().bytes().readPosition();
                        try {
                            read.marshallable(v);
                        } catch (Exception e) {

                            @NotNull final String msg = dc.wire().bytes().toHexString(readPosition, dc.wire()
                                    .bytes()
                                    .readLimit() - readPosition);

                            LOG.error("Error passing " + v.getClass().getSimpleName() + " bytes:\n"
                                    + msg, e);
                            return false;
                        }

                        final Object k;
                        if (v instanceof KeyedMarshallable) {
                            final Bytes bytes = Wires.acquireBytes();
                            ((KeyedMarshallable) v).writeKey(bytes);
                            k = bytesToKey.computeIfAbsent(bytes, Bytes::copy);
                        } else
                            continue;

                        if (k == null)
                            continue;

                        messagesReadPerSecond++;

                        @NotNull final String eventName = sb.toString();
                        synchronized (lastIndexLock) {
                            multiMap.computeIfAbsent(eventName, e -> new ConcurrentHashMap<>())
                                    .compute(k, (k1, vOld) -> {
                                        if (vOld == null)
                                            return new IndexedValue<>(deepCopy(v), dc.index());
                                        else {
                                            copyTo(v, vOld.v());
                                            vOld.index(dc.index());
                                            return vOld;
                                        }
                                    });
                            lastIndexRead = dc.index();
                        }
                    }

                } catch (RuntimeException e) {
                    Jvm.warn().on(getClass(), fromSizePrefixedBlobs(dc.wire().bytes(), start - 4), e);
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

        @NotNull final ExcerptTailer tailer = chronicleQueue.createTailer();
        final long start = tailer.toStart().index();
        @NotNull final ExcerptTailer excerptTailer = tailer.toEnd();
        final long endIndex = excerptTailer.index();

        long fromIndex0 = vanillaIndexQuery.fromIndex();
        if (fromIndex0 == FROM_START) {
            @NotNull final RollingChronicleQueue chronicleQueue = (RollingChronicleQueue) this.chronicleQueue;
            RollCycle rollCycle = chronicleQueue.rollCycle();
            int currentIndex = rollCycle.current(SystemTimeProvider.INSTANCE, 0);
            final int cycle = rollCycle.toCycle(currentIndex);
            fromIndex0 = rollCycle.toIndex(cycle, 0);
        } else if (fromIndex0 == 0) {
            fromIndex0 = endIndex;
        }

        fromIndex0 = Math.min(fromIndex0, endIndex);
        fromIndex0 = Math.max(fromIndex0, start);

        final long fromIndex = fromIndex0;
        final boolean success = tailer.moveToIndex(fromIndex);

        assert success || (fromIndex == endIndex) : "fromIndex=" + Long.toHexString(fromIndex)
                + ", start=" + Long.toHexString(start) + ",end=" + Long.toHexString(endIndex);

        if (fromIndex <= endIndex) {
            registerSubscriber(sub, vanillaIndexQuery, tailer, fromIndex);
            return;
        }

        // the method below ensures that all the data is loaded up-to the the current tail, as
        // the user has asked for an index that we have not yet loaded
        ensureAllDataIsLoadedBeforeRegistingSubsribe(sub, vanillaIndexQuery, tailer, endIndex, fromIndex);
    }

    private void ensureAllDataIsLoadedBeforeRegistingSubsribe(@NotNull ConsumingSubscriber<IndexedValue<V>> sub, @NotNull IndexQuery<V> vanillaIndexQuery, @NotNull ExcerptTailer tailer, long endIndex, long fromIndex) {
        @Nullable final EventLoop eventLoop = asset.root().getView(EventLoop.class);
        eventLoop.addHandler(() -> endOfTailCheckedRegisterSubscriber(sub, vanillaIndexQuery, tailer, endIndex, fromIndex));
    }

    /**
     * waits till the queue is read up-to the current tail index before proceeding
     */
    private boolean endOfTailCheckedRegisterSubscriber(@NotNull ConsumingSubscriber<IndexedValue<V>> sub,
                                                       @NotNull IndexQuery<V> vanillaIndexQuery,
                                                       @NotNull ExcerptTailer tailer,
                                                       long endIndex, long fromIndex) throws InvalidEventHandlerException {
        if (lastIndexRead > endIndex)
            return false;

        registerSubscriber(sub, vanillaIndexQuery, tailer, fromIndex);
        throw new InvalidEventHandlerException();
    }


    private static class CheckPointPredicate implements Predicate<IndexedValue>, LongSupplier {
        private long fromIndex0;
        private long max0;

        private CheckPointPredicate(long fromIndex0) {
            this.fromIndex0 = fromIndex0;
        }

        @Override
        public long getAsLong() {
            return max0;
        }


        @Override
        public boolean test(IndexedValue i) {

            if (i.index() >= fromIndex0) {
                max0 = Math.max(max0, i.index());
                return false;
            }

            return true;
        }
    }


    private void registerSubscriber(@NotNull ConsumingSubscriber<IndexedValue<V>> sub, @NotNull IndexQuery<V> vanillaIndexQuery, @NotNull ExcerptTailer tailer, long fromIndex) {
        @NotNull final AtomicBoolean isClosed = new AtomicBoolean();
        activeSubscriptions.put(sub, isClosed);

        final String eventName = vanillaIndexQuery.eventName();
        final Predicate<V> filter = vanillaIndexQuery.filter();

        // don't set iterator if the 'fromIndex' has not caught up.

        @NotNull final Iterator<IndexedValue<V>> iterator;

        final ConcurrentMap<Object, IndexedValue<V>> map = multiMap.computeIfAbsent(eventName, k -> new ConcurrentHashMap<>());

        final long fromIndex0 = fromIndex;
        CheckPointPredicate checkPointPredicate = new CheckPointPredicate(fromIndex0);
        iterator = (vanillaIndexQuery.bootstrap())
                ? map.values().stream().filter(
                i -> filter.test(i.v()) && checkPointPredicate.test(i)).iterator()
                : EMPTY_ITERATOR;

        try {
            @NotNull final Supplier<Marshallable> supplier = excerptConsumer(vanillaIndexQuery,
                    tailer, iterator, fromIndex, checkPointPredicate);
            sub.addSupplier(supplier);

        } catch (RuntimeException e) {
            sub.onEndOfSubscription();
            Jvm.warn().on(getClass(), "Error registering subscription", e);
        }
    }

    @NotNull
    private Supplier<Marshallable> excerptConsumer(@NotNull IndexQuery<V> vanillaIndexQuery,
                                                   @NotNull ExcerptTailer tailer,
                                                   @NotNull Iterator<IndexedValue<V>> iterator,
                                                   final long fromIndex,
                                                   LongSupplier lastIndexOfSnapshot) {
        return () -> VanillaIndexQueueView.this.value(vanillaIndexQuery, tailer, iterator, fromIndex, lastIndexOfSnapshot);
    }

    @Nullable
    private Marshallable value(@NotNull IndexQuery<V> vanillaIndexQuery,
                               @NotNull ExcerptTailer tailer,
                               @NotNull Iterator<IndexedValue<V>> iterator,
                               final long from,
                               @NotNull final LongSupplier lastIndexOfSnapshot) {

        if (iterator.hasNext()) {
            IndexedValue<V> indexedValue = iterator.next();
            indexedValue.timePublished(System.currentTimeMillis());
            indexedValue.maxIndex(lastIndexRead);
            // we have to also check that we are on the last message
            // because the value returned  by lastIndexOfSnapshot may change on each call
            // as more of the maps is understood
            boolean endOfSnapshot = !iterator.hasNext() && lastIndexOfSnapshot.getAsLong() == indexedValue.index();
            indexedValue.isEndOfSnapshot(endOfSnapshot);
            return indexedValue;
        }

        final String eventName = vanillaIndexQuery.eventName();
        final Predicate<V> filter = vanillaIndexQuery.filter();
        if (isClosed.get())
            throw Jvm.rethrow(new InvalidEventHandlerException("shutdown"));

        try (DocumentContext dc = tailer.readingDocument()) {
            try {
                if (!dc.isPresent())
                    return null;

                if (LOG.isDebugEnabled())
                    Jvm.debug().on(getClass(), "processing the following message=" + fromSizePrefixedBlobs(dc));

                // we may have just been restated and have not yet caught up
                if (from > dc.index())
                    return null;

                Class<? extends Marshallable> type = typeToString.toType(eventName);
                if (type == null)
                    return null;

                final StringBuilder serialisedEventName = eventNameDeserialiserPool.acquireStringBuilder();
                @NotNull final ValueIn valueIn = dc.wire().read(serialisedEventName);

                if (valueIn instanceof DefaultValueIn)
                    return null;

                if (!eventNamesMatch(serialisedEventName, eventName)) {
                    return null;
                }

                @NotNull final V v = (V) VanillaObjectCacheFactory.INSTANCE.get()
                        .apply(type);
                valueIn.marshallable(v);

                if (!filter.test(v))
                    return null;

                final IndexedValue<V> indexedValue = this.indexedValue.get();
                long index = dc.index();
                indexedValue.index(index);
                indexedValue.v(v);
                indexedValue.timePublished(System.currentTimeMillis());
                indexedValue.isEndOfSnapshot(indexedValue == lastIndexOfSnapshot);
                indexedValue.maxIndex(Math.max(dc.index(), lastIndexRead));
                return indexedValue;

            } finally {
                if (dc.isPresent())
                    // required for delta-wire, as it has to consume all the the fields
                    while (dc.wire().hasMore()) {
                        dc.wire().read().skipValue();
                    }
            }

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

    private static boolean eventNamesMatch(final CharSequence serialisedEventName,
                                           final CharSequence queryEventName) {
        if (serialisedEventName.length() != queryEventName.length()) {
            return false;
        }

        for (int i = 0; i < serialisedEventName.length(); i++) {
            if (serialisedEventName.charAt(i) != queryEventName.charAt(i)) {
                return false;
            }
        }

        return true;
    }
}