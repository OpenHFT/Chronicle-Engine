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

package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.*;
import net.openhft.chronicle.engine.api.set.EntrySetView;
import net.openhft.chronicle.engine.api.set.KeySetView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.fs.Clusters;
import net.openhft.chronicle.engine.fs.EngineCluster;
import net.openhft.chronicle.engine.fs.EngineHostDetails;
import net.openhft.chronicle.engine.map.VanillaKeyValueStore;
import net.openhft.chronicle.engine.map.VanillaMapView;
import net.openhft.chronicle.engine.pubsub.QueueTopicPublisher;
import net.openhft.chronicle.engine.query.Filter;
import net.openhft.chronicle.engine.query.QueueConfig;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.util.*;
import java.util.function.BiConsumer;

import static net.openhft.chronicle.core.util.ObjectUtils.convertTo;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.defaultZeroBinary;
import static net.openhft.chronicle.wire.WireType.*;

/**
 * @author Rob Austin.
 */
public class ChronicleQueueView<T, M> implements QueueView<T, M>, MapView<T, M>, SubAssetFactory,
        Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ChronicleQueueView.class);

    @NotNull
    private final RollingChronicleQueue chronicleQueue;
    private final Class<T> messageTypeClass;
    @NotNull
    private final Class<M> elementTypeClass;
    private final ThreadLocal<ThreadLocalData> threadLocal;
    @NotNull
    private final String defaultPath;
    @NotNull
    private final RequestContext context;
    @NotNull
    private final Asset asset;
    private boolean isSource;
    private boolean isReplicating;
    private boolean dontPersist;

    @NotNull
    private QueueConfig queueConfig;

    private volatile MapView<T, M> mapView;

    public ChronicleQueueView(@NotNull RequestContext context,
                              @NotNull Asset asset) throws IOException {
        this(null, context, asset);
    }

    public ChronicleQueueView(@Nullable RollingChronicleQueue queue,
                              @NotNull RequestContext context,
                              @NotNull Asset asset) throws IOException {
        this.context = context;
        this.asset = asset;
        @NotNull String s = asset.fullName();
        if (s.startsWith("/")) s = s.substring(1);
        defaultPath = s;
        @Nullable final HostIdentifier hostIdentifier = asset.findOrCreateView(HostIdentifier.class);
        @Nullable final Byte hostId = hostIdentifier == null ? null : hostIdentifier.hostId();

        try {
            queueConfig = asset.findView(QueueConfig.class);
        } catch (AssetNotFoundException anfe) {
            Jvm.debug().on(getClass(), "queue config not found asset=" + asset.fullName());
            throw anfe;
        }

        chronicleQueue = queue != null ? queue : newInstance(context.name(), context.basePath(), hostId, queueConfig.wireType());
        messageTypeClass = context.messageType();
        elementTypeClass = context.elementType();
        threadLocal = ThreadLocal.withInitial(() -> new ThreadLocalData(chronicleQueue));
        dontPersist = context.dontPersist();

        if (hostId != null)
            replication(context, asset);

        @Nullable EventLoop eventLoop = asset.findOrCreateView(EventLoop.class);
        eventLoop.addHandler(new EventHandler() {
            @Override
            public boolean action() throws InvalidEventHandlerException, InterruptedException {
                chronicleQueue.acquireAppender().pretouch();
                return false;
            }

            @NotNull
            @Override
            public HandlerPriority priority() {
                return HandlerPriority.MONITOR;
            }
        });
    }

    @NotNull
    @SuppressWarnings("WeakerAccess")
    public static WriteMarshallable newSource(long nextIndexRequired,
                                              @NotNull Class topicType,
                                              @NotNull Class elementType,
                                              boolean acknowledgement,
                                              @Nullable MessageAdaptor messageAdaptor) {
        Objects.requireNonNull(topicType);
        Objects.requireNonNull(elementType);
        try {
            Class<?> aClass = Class.forName("software.chronicle.enterprise.queue.QueueSourceReplicationHandler");
            Constructor<?> declaredConstructor = aClass.getDeclaredConstructor(long.class, Class.class,
                    Class.class, boolean.class, MessageAdaptor.class);
            return (WriteMarshallable) declaredConstructor.newInstance(nextIndexRequired,
                    topicType, elementType, acknowledgement, messageAdaptor);

        } catch (Exception e) {
            @NotNull IllegalStateException licence = new IllegalStateException("A Chronicle Queue Enterprise licence is" +
                    " required to run chronicle-queue replication. Please contact sales@chronicle" +
                    ".software");
            Jvm.warn().on(ChronicleQueueView.class, licence.getMessage());
            throw licence;
        }
    }

    /**
     * @param topicType       the type of the topic
     * @param elementType     the type of the element
     * @param acknowledgement {@code true} if message acknowledgement to the source is required
     * @param messageAdaptor  used to apply processing in the bytes before they are written to the
     *                        queue
     * @return and instance of QueueSyncReplicationHandler
     */
    @NotNull
    @SuppressWarnings("WeakerAccess")
    public static WriteMarshallable newSync(
            @NotNull Class topicType,
            @NotNull Class elementType,
            boolean acknowledgement,
            @Nullable MessageAdaptor messageAdaptor,
            @NotNull WireType wireType) {
        try {

            Class<?> aClass = Class.forName("software.chronicle.enterprise.queue.QueueSyncReplicationHandler");
            Constructor<?> declaredConstructor = aClass.getConstructor(Class.class, Class.class,
                    boolean.class, MessageAdaptor.class, WireType.class);
            return (WriteMarshallable) declaredConstructor.newInstance(topicType, elementType,
                    acknowledgement, messageAdaptor, wireType);

        } catch (Exception e) {
            @NotNull IllegalStateException licence = new IllegalStateException("A Chronicle Queue Enterprise licence is" +
                    " required to do chronicle-queue replication. " +
                    "Please contact sales@chronicle.software");
            Jvm.warn().on(ChronicleQueueView.class, licence.getMessage());
            throw licence;
        }
    }

    public static boolean isQueueReplicationAvailable() {
        try {
            Class.forName("software.chronicle.enterprise.queue.QueueSyncReplicationHandler");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    private static void deleteFiles(@NotNull File element) throws IOException {
        if (element.isDirectory()) {
            @Nullable File[] files = element.listFiles();
            if (files == null)
                return;
            for (@NotNull File sub : files) {
                deleteFiles(sub);
            }
        }
        try {
            Files.deleteIfExists(element.toPath());

        } catch (IOException e) {
            Jvm.debug().on(ChronicleQueueView.class, "Unable to delete " + element, e);
        }
    }

    @NotNull
    public static QueueView create(@NotNull RequestContext context, @NotNull Asset asset) {
        try {
            return new ChronicleQueueView<>(context, asset);
        } catch (IOException e) {
            throw Jvm.rethrow(e);
        }
    }

    public MapView<T, M> mapView() {
        final MapView<T, M> mapView = this.mapView;

        if (mapView != null) {
            return mapView;
        }

        synchronized (this) {
            MapView<T, M> mapView0 = this.mapView;
            if (mapView0 != null)
                return mapView0;

            this.mapView = new QueueViewAsMapView<T, M>(this, context, asset);
            return this.mapView;
        }

    }

    @Nullable
    public RollingChronicleQueue chronicleQueue() {
        return chronicleQueue;
    }

    public void replication(@NotNull RequestContext context, @NotNull Asset asset) {
        @Nullable final HostIdentifier hostIdentifier;

        try {
            hostIdentifier = asset.findOrCreateView(HostIdentifier.class);
        } catch (AssetNotFoundException anfe) {
            if (LOG.isDebugEnabled())
                Jvm.debug().on(getClass(), "replication not enabled " + anfe.getMessage());
            return;
        }

        final int remoteSourceIdentifier = queueConfig.sourceHostId(context.fullName());
        isSource = hostIdentifier.hostId() == remoteSourceIdentifier;
        isReplicating = true;

        @Nullable final Clusters clusters = asset.findView(Clusters.class);

        if (clusters == null) {
            LOG.warn("no cluster found name=" + context.cluster());
            Jvm.debug().on(getClass(), "no cluster found name=" + context.cluster());
            return;
        }

        final EngineCluster engineCluster = clusters.get(context.cluster());
        @NotNull final String csp = context.fullName();

        if (engineCluster == null) {
            Jvm.debug().on(getClass(), "no cluster found name=" + context.cluster());
            LOG.warn("no cluster found name=" + context.cluster());
            return;
        }

        byte localIdentifier = hostIdentifier.hostId();

        if (LOG.isDebugEnabled())
            Jvm.debug().on(getClass(), "hostDetails : localIdentifier=" + localIdentifier + ",cluster=" + engineCluster.hostDetails());

        // if true - each replication event sends back an enableAcknowledgment
        final boolean acknowledgement = queueConfig.acknowledgment();
        final MessageAdaptor messageAdaptor = queueConfig.bytesFunction();

        for (@NotNull EngineHostDetails hostDetails : engineCluster.hostDetails()) {

            // its the identifier with the larger values that will establish the connection
            byte remoteIdentifier = (byte) hostDetails.hostId();

            if (remoteIdentifier == localIdentifier)
                continue;

            engineCluster.findConnectionManager(remoteIdentifier).addListener((nc, isConnected) -> {

                if (!isConnected)
                    return;

                if (nc.isAcceptor())
                    return;

                final boolean isSource0 = (remoteIdentifier == remoteSourceIdentifier);

                @NotNull WriteMarshallable h = isSource0 ?

                        newSource(chronicleQueue.createTailer().toEnd().index(), context.topicType(),
                                context.elementType(),
                                acknowledgement,
                                messageAdaptor) :

                        newSync(context.topicType(),
                                context.elementType(),
                                acknowledgement,
                                messageAdaptor,
                                chronicleQueue.wireType());

                long cid = nc.newCid();
                nc.wireOutPublisher().publish(w -> w.writeDocument(true, d ->
                        d.writeEventName(CoreFields.csp).text(csp)
                                .writeEventName(CoreFields.cid).int64(cid)
                                .writeEventName(CoreFields.handler).typedMarshallable(h)));

            });
        }
    }

    @NotNull
    @Override
    public KeySetView<T> keySet() {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    @Override
    public Collection<M> values() {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    @Override
    public EntrySetView<T, Object, M> entrySet() {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    @Override
    public M getUsing(T key, Object using) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void registerTopicSubscriber(@NotNull TopicSubscriber<T, M> topicSubscriber) throws
            AssetNotFoundException {
        asset.registerTopicSubscriber(asset.fullName(), context.type(), context.type2(), topicSubscriber);
    }

    @Override
    public void unregisterTopicSubscriber(@NotNull TopicSubscriber<T, M> topicSubscriber) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void registerKeySubscriber(@NotNull Subscriber<T> subscriber) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void registerKeySubscriber(@NotNull Subscriber<T> subscriber, @NotNull Filter filter, @NotNull Set<RequestContext.Operation> contextOperations) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void registerSubscriber(@NotNull Subscriber<MapEvent<T, M>> subscriber, @NotNull Filter<MapEvent<T, M>> filter, @NotNull Set<RequestContext.Operation> contextOperations) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Reference<M> referenceFor(T key) {
        return mapView().referenceFor(key);
    }

    @Override
    public Class<T> keyType() {
        return mapView().keyType();
    }

    @Override
    public Class<M> valueType() {
        return mapView().valueType();
    }

    @Override
    public long longSize() {
        return mapView().longSize();
    }

    @NotNull
    @Override
    public M getAndPut(T key, M value) {
        return getAndPut(key, value);
    }

    @Nullable
    @Override
    public M getAndRemove(T key) {
        return mapView().getAndRemove(key);
    }

    public void unregisterTopicSubscriber(@NotNull T topic, @NotNull TopicSubscriber<T, M> topicSubscriber) {
        @NotNull String name = "".equals(topic.toString().trim())
                ? asset.fullName() : asset
                .fullName() + "/" + topic.toString();
        asset.unregisterTopicSubscriber(name, context.type(), context.type2(), topicSubscriber);
    }

    @NotNull
    @Override
    public Publisher<M> publisher(@NotNull T topic) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void registerSubscriber(@NotNull T topic, @NotNull Subscriber<M> subscriber) {
        @NotNull String name = "".equals(topic.toString().trim())
                ? asset.fullName() : asset
                .fullName() + "/" + topic.toString();

        asset.registerTopicSubscriber(name, context.type(), context.type2(),
                (topic1, message) -> subscriber.onMessage((M) message));
    }

    private RollingChronicleQueue newInstance(@NotNull String name,
                                              @Nullable String basePath,
                                              @Nullable Byte hostID,
                                              @NotNull WireType wireType) throws IOException {

        if (wireType == DELTA_BINARY)
            throw new IllegalArgumentException("Chronicle Queues can not be set to use delta wire");

        if (wireType != BINARY && wireType != DEFAULT_ZERO_BINARY)
            throw new IllegalArgumentException("Currently the chronicle queue only supports Binary and Default Zero Binary Wire");

        RollingChronicleQueue chronicleQueue;

        @Nullable File baseFilePath;
        if (basePath == null)
            baseFilePath = new File(defaultPath, "");
        else
            baseFilePath = new File(basePath);

        if (!baseFilePath.exists())
            Files.createDirectories(baseFilePath.toPath());

        @NotNull final SingleChronicleQueueBuilder builder = wireType == DEFAULT_ZERO_BINARY
                ? defaultZeroBinary(baseFilePath)
                : binary(baseFilePath);
        // TODO make configurable
//        builder.blockSize(256 << 20);

        return builder.build();
    }

    @NotNull
    private ExcerptTailer threadLocalTailer() {
        return threadLocal.get().tailer;
    }

    @NotNull
    private ExcerptAppender threadLocalAppender() {
        return threadLocal.get().appender;
    }

    @Nullable
    public Tailer<T, M> tailer() {
        @NotNull final ExcerptTailer tailer = ChronicleQueueView.this.chronicleQueue.createTailer();
        @NotNull final LocalExcept localExcept = new LocalExcept();
        return () -> ChronicleQueueView.this.next(tailer, localExcept);
    }

    private Excerpt<T, M> next(@NotNull ExcerptTailer excerptTailer, @NotNull final LocalExcept excerpt) {
        excerpt.clear();
        try (DocumentContext dc = excerptTailer.readingDocument()) {
            if (!dc.isPresent())
                return null;
            final Wire wire = dc.wire();
            long pos = wire.bytes().readPosition();
            final T topic = wire.readEvent(messageTypeClass);
            @NotNull final ValueIn valueIn = wire.getValueIn();
            if (Bytes.class.isAssignableFrom(elementTypeClass)) {
                valueIn.text(excerpt.text());
            } else {
                @Nullable final M message = valueIn.object(elementTypeClass);
                excerpt.message(message);
                System.out.println(pos + " " + topic + " " + message);
            }
            return excerpt
                    .topic(topic)
                    .index(excerptTailer.index());
        }
    }

    /**
     * @param index gets the except at the given index, if index==0 then the first index is
     *              returned
     * @return the except
     */
    @Nullable
    @Override
    public Excerpt<T, M> getExcerpt(long index) {
        final ThreadLocalData threadLocalData = threadLocal.get();
        @NotNull ExcerptTailer excerptTailer = threadLocalData.replayTailer;

        if (index == 0)
            excerptTailer.toStart();
        else if (!excerptTailer.moveToIndex(index))
            return null;

        try (DocumentContext dc = excerptTailer.readingDocument()) {
            if (!dc.isPresent())
                return null;
            final StringBuilder topic = Wires.acquireStringBuilder();
            @Nullable final M message = dc.wire().readEventName(topic).object(elementTypeClass);

            return threadLocalData.excerpt
                    .message(message)
                    .topic(convertTo(messageTypeClass, topic))
                    .index(excerptTailer.index());
        }
    }

    @Nullable
    @Override
    public Excerpt<T, M> getExcerpt(@NotNull T topic) {

        final ThreadLocalData threadLocalData = threadLocal.get();
        @NotNull ExcerptTailer excerptTailer = threadLocalData.replayTailer;
        for (; ; ) {

            try (DocumentContext dc = excerptTailer.readingDocument()) {
                if (!dc.isPresent())
                    return null;
                final StringBuilder t = Wires.acquireStringBuilder();
                @NotNull final ValueIn valueIn = dc.wire().readEventName(t);

                @Nullable final T topic1 = convertTo(messageTypeClass, t);

                if (!topic.equals(topic1))
                    continue;

                @Nullable final M message = valueIn.object(elementTypeClass);

                return threadLocalData.excerpt
                        .message(message)
                        .topic(topic1)
                        .index(excerptTailer.index());
            }
        }
    }

    @Override
    public void set(T key, M element) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void publish(@NotNull T topic, @NotNull M message) {
        publishAndIndex(topic, message);
    }

    /**
     * @param consumer a consumer that provides that name of the event and value contained within
     *                 the except
     */
    public void getExcerpt(@NotNull BiConsumer<CharSequence, M> consumer) {
        @NotNull final ExcerptTailer tailer = threadLocalTailer();

        tailer.readDocument(w -> {
            final StringBuilder eventName = Wires.acquireStringBuilder();
            @NotNull final ValueIn valueIn = w.readEventName(eventName);
            consumer.accept(eventName, valueIn.object(elementTypeClass));

        });
    }

    public long publishAndIndex(@NotNull T topic, @NotNull M message) {

        if (isReplicating && !isSource)
            throw new IllegalStateException("You can not publish to a sink used in replication, " +
                    "you have to publish to the source");

        @NotNull final ExcerptAppender excerptAppender = threadLocalAppender();

        try (final DocumentContext dc = excerptAppender.writingDocument()) {
            dc.wire().writeEvent(messageTypeClass, topic).object(elementTypeClass, message);
        }
        return excerptAppender.lastIndexAppended();
    }

    public long set(@NotNull M event) {
        if (isReplicating && !isSource)
            throw new IllegalStateException("You can not publish to a sink used in replication, " +
                    "you have to publish to the source");
        @NotNull final ExcerptAppender excerptAppender = threadLocalAppender();
        excerptAppender.writeDocument(w -> w.writeEventName(() -> "").object(event));
        return excerptAppender.lastIndexAppended();
    }

    @Override
    public boolean isEmpty() {
        return mapView().isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return mapView().containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return mapView().containsValue(value);
    }

    @Override
    public M get(Object key) {
        return mapView().get(key);
    }

    @Override
    public M put(T key, M value) {
        return mapView().put(key, value);
    }

    @Override
    public M remove(Object key) {
        return mapView().remove(key);
    }

    @Override
    public void putAll(@NotNull Map<? extends T, ? extends M> m) {
        mapView().putAll(m);
    }

    public void clear() {
        chronicleQueue.clear();
        mapView().clear();
    }

    @NotNull
    public File path() {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    public WireType wireType() {
        throw new UnsupportedOperationException("todo");
    }

    public void close() {

        @NotNull File file = chronicleQueue.file();
        chronicleQueue.close();
        if (dontPersist) {
            try {
                deleteFiles(file);

            } catch (Exception e) {
                Jvm.debug().on(getClass(), "Unable to delete " + file, e);
            }
        }
    }

    private void deleteFiles(TopicPublisher p) {

        if (p instanceof QueueTopicPublisher)
            deleteFiles((ChronicleQueueView) ((QueueTopicPublisher) p).underlying());

    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        Closeable.closeQuietly(this);
    }

    @Override
    public void registerSubscriber(@NotNull Subscriber<MapEvent<T, M>> subscriber) {
        mapView().registerSubscriber(subscriber);
    }

    public void unregisterSubscriber(Subscriber subscriber) {

    }

    public int subscriberCount() {
        throw new UnsupportedOperationException("todo");
    }

    public String dump() {
        return chronicleQueue.dump();
    }

    @Nullable
    @Override
    public <E> Asset createSubAsset(@NotNull VanillaAsset vanillaAsset, String name, Class<E> valueType) {
        return new VanillaSubAsset<>(vanillaAsset, name, valueType, null);
    }

    @Override
    public M putIfAbsent(@NotNull T key, M value) {
        return mapView().putIfAbsent(key, value);
    }

    @Override
    public boolean remove(@NotNull Object key, Object value) {
        return mapView().remove(key, value);
    }

    @Override
    public boolean replace(@NotNull T key, @NotNull M oldValue, @NotNull M newValue) {
        return mapView().replace(key, oldValue, newValue);
    }

    @Override
    public M replace(@NotNull T key, @NotNull M value) {
        return mapView().replace(key, value);
    }

    @Override
    public Asset asset() {
        return mapView().asset();
    }

    @Nullable
    @Override
    public Object underlying() {
        return chronicleQueue;
    }

    public static class LocalExcept<T, M> implements Excerpt<T, M>, Marshallable, Map.Entry<T, M> {
        @Nullable
        private T topic;
        @Nullable
        private M message;
        private Bytes bytes;
        private long index;

        @Nullable
        @Override
        public T topic() {
            return topic;
        }

        @Nullable
        @Override
        public M message() {
            return message;
        }

        @Override
        public long index() {
            return this.index;
        }

        @NotNull
        public LocalExcept<T, M> index(long index) {
            this.index = index;
            return this;
        }

        @NotNull
        LocalExcept message(M message) {
            this.message = message;
            return this;
        }

        @NotNull
        LocalExcept topic(T topic) {
            this.topic = topic;
            return this;
        }

        @NotNull
        @Override
        public String toString() {
            return "Except{" + "topic=" + topic + ", message=" + message + '}';
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wireOut) {
            wireOut.write(() -> "topic").object(topic);
            wireOut.write(() -> "message").object(message);
            wireOut.write(() -> "index").int64(index);
        }

        @Override
        public void readMarshallable(@NotNull WireIn wireIn) throws IORuntimeException {
            topic((T) wireIn.read(() -> "topic").object(Object.class));
            message((M) wireIn.read(() -> "message").object(Object.class));
            index(wireIn.read(() -> "index").int64());
        }

        public void clear() {
            message = null;
            topic = null;
            index = -1;
        }

        public Bytes text() {
            if (bytes == null)
                bytes = Bytes.allocateElasticDirect();
            else
                bytes.clear();
            message = (M) bytes;
            return bytes;
        }

        @Nullable
        @Override
        public T getKey() {
            return topic;
        }

        @Nullable
        @Override
        public M getValue() {
            return message;
        }

        @NotNull
        @Override
        public M setValue(M value) {
            throw new UnsupportedOperationException("todo");
        }
    }

    /**
     * provides mapView view support for a Queue View
     *
     * @param <K>
     * @param <V>
     */
    private static class QueueViewAsMapView<K, V> extends VanillaMapView<K, V> {

        @NotNull
        private final QueueView<K, V> queueView;

        QueueViewAsMapView(@NotNull final QueueView<K, V> queueView,
                           @NotNull RequestContext context,
                           @NotNull Asset asset) {
            super(context, asset, new VanillaKeyValueStore<>(context, asset));
            this.queueView = queueView;

            queueView.registerTopicSubscriber((topic, message) -> {
                if (message == null)
                    super.remove(topic);
                else
                    super.put(topic, message);
            });

        }

        @Nullable
        @Override
        public V put(@NotNull K key, @NotNull V value) {
            if (putReturnsNull) {
                queueView.publishAndIndex(key, value);
                super.put(key, value);
                return null;
            } else {
                @Nullable V v = super.get(key);
                queueView.publishAndIndex(key, value);
                super.put(key, value);
                return v;
            }
        }

        @Override
        public void set(K key, @NotNull V value) {
            queueView.publishAndIndex((K) key, value);
            super.put(key, value);
        }

        @Override
        public V remove(Object key) {

            if (removeReturnsNull) {
                queueView.publishAndIndex((K) key, null);
                super.remove(key);
                return null;
            } else {
                @Nullable V v = super.get(key);
                queueView.publishAndIndex((K) key, null);
                super.remove(key);
                return v;
            }
        }

        @SuppressWarnings("WhileLoopReplaceableByForEach")
        @Override
        public void clear() {
            @NotNull Iterator<Entry<K, V>> iterator = entrySet().iterator();

            while (iterator.hasNext()) {
                remove(iterator.next().getKey());
            }
        }

        @Nullable
        @Override
        public V putIfAbsent(@net.openhft.chronicle.core.annotation.NotNull K key, @NotNull V value) {
            checkKey(key);
            checkValue(value);
            @Nullable V v = super.putIfAbsent(key, value);
            if (v != null)
                queueView.publishAndIndex((K) key, value);
            return v;
        }

        @Override
        public boolean remove(@net.openhft.chronicle.core.annotation.NotNull Object key, Object value) {
            if (!super.remove(key, value))
                return false;
            queueView.publishAndIndex((K) key, null);
            return true;
        }

        @Override
        public boolean replace(@net.openhft.chronicle.core.annotation.NotNull K key,
                               @net.openhft.chronicle.core.annotation.NotNull V oldValue,
                               @net.openhft.chronicle.core.annotation.NotNull V newValue) {
            if (!super.replace(key, oldValue, newValue))
                return false;

            queueView.publishAndIndex((K) key, newValue);
            return true;
        }

        @Nullable
        @Override
        public V replace(@net.openhft.chronicle.core.annotation.NotNull K key,
                         @net.openhft.chronicle.core.annotation.NotNull V value) {
            @Nullable V replaced = super.replace(key, value);
            queueView.publishAndIndex((K) key, value);
            return replaced;
        }

    }

    class ThreadLocalData {

        @NotNull
        final ExcerptAppender appender;
        @NotNull
        final ExcerptTailer tailer;
        @NotNull
        final ExcerptTailer replayTailer;
        @NotNull
        final LocalExcept excerpt;

        ThreadLocalData(@NotNull ChronicleQueue chronicleQueue) {
            appender = chronicleQueue.acquireAppender();
            appender.padToCacheAlign(net.openhft.chronicle.wire.MarshallableOut.Padding.ALWAYS);
            tailer = chronicleQueue.createTailer();

            replayTailer = chronicleQueue.createTailer();
            excerpt = new LocalExcept();
        }
    }

}

