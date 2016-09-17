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
import net.openhft.chronicle.engine.api.pubsub.Publisher;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.TopicPublisher;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.fs.Clusters;
import net.openhft.chronicle.engine.fs.EngineCluster;
import net.openhft.chronicle.engine.fs.EngineHostDetails;
import net.openhft.chronicle.engine.pubsub.QueueTopicPublisher;
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
import java.util.function.BiConsumer;

import static net.openhft.chronicle.core.util.ObjectUtils.convertTo;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.defaultZeroBinary;
import static net.openhft.chronicle.wire.WireType.*;

/**
 * @author Rob Austin.
 */
public class ChronicleQueueView<T, M> implements QueueView<T, M>, SubAssetFactory, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ChronicleQueueView.class);

    private final RollingChronicleQueue chronicleQueue;
    private final Class<T> messageTypeClass;
    @NotNull
    private final Class<M> elementTypeClass;
    private final ThreadLocal<ThreadLocalData> threadLocal;
    private final String defaultPath;

    private boolean isSource;
    private boolean isReplicating;
    private boolean dontPersist;

    @NotNull
    private QueueConfig queueConfig;

    private ChronicleQueueView(@NotNull RequestContext context, @NotNull Asset asset) throws IOException {
        this(null, context, asset);
    }

    private ChronicleQueueView(@Nullable RollingChronicleQueue queue,
                               @NotNull RequestContext context,
                               @NotNull Asset asset) throws IOException {

        String s = asset.fullName();
        if (s.startsWith("/")) s = s.substring(1);
        defaultPath = s;
        final HostIdentifier hostIdentifier = asset.findOrCreateView(HostIdentifier.class);
        final Byte hostId = hostIdentifier == null ? null : hostIdentifier.hostId();

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

        EventLoop eventLoop = asset.findView(EventLoop.class);
        eventLoop.addHandler(new EventHandler() {
            @Override
            public boolean action() throws InvalidEventHandlerException, InterruptedException {
                chronicleQueue.acquireAppender().pretouch();
                return false;
            }

            @Override
            public HandlerPriority priority() {
                return HandlerPriority.MONITOR;
            }
        });
    }

    @NotNull
    public static String resourcesDir() {
        String path = ChronicleQueueView.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        if (path == null)
            return ".";
        return new File(path).getParentFile().getParentFile() + "/src/test/resources";
    }

    private static WriteMarshallable newSource(long nextIndexRequired,
                                               @NotNull Class topicType,
                                               @NotNull Class elementType,
                                               boolean acknowledgement,
                                               @Nullable MessageAdaptor messageAdaptor) {
        try {
            Class<?> aClass = Class.forName("software.chronicle.enterprise.queue.QueueSourceReplicationHandler");
            Constructor<?> declaredConstructor = aClass.getDeclaredConstructor(long.class, Class.class,
                    Class.class, boolean.class, MessageAdaptor.class);
            return (WriteMarshallable) declaredConstructor.newInstance(nextIndexRequired,
                    topicType, elementType, acknowledgement, messageAdaptor);

        } catch (Exception e) {
            IllegalStateException licence = new IllegalStateException("A Chronicle Queue Enterprise licence is" +
                    " required to run this code. Please contact sales@chronicle.software");
            Jvm.warn().on(ChronicleQueueView.class, e);
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
    private static WriteMarshallable newSync(
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
            IllegalStateException licence = new IllegalStateException("A Chronicle Queue Enterprise licence is" +
                    " required to run this code." +
                    "Please contact sales@chronicle.software");
            Jvm.warn().on(ChronicleQueueView.class, e);
            throw licence;
        }
    }

    private static void deleteFiles(File element) throws IOException {
        if (element.isDirectory()) {
            File[] files = element.listFiles();
            if (files == null)
                return;
            for (File sub : files) {
                deleteFiles(sub);
            }
        }
        try {
            Files.deleteIfExists(element.toPath());

        } catch (IOException e) {
            Jvm.debug().on(ChronicleQueueView.class, "Unable to delete " + element, e);
        }
    }

    public static QueueView create(RequestContext context, Asset asset) {
        try {
            return new ChronicleQueueView<>(context, asset);
        } catch (IOException e) {
            throw Jvm.rethrow(e);
        }
    }

    public RollingChronicleQueue chronicleQueue() {
        return chronicleQueue;
    }

    public void replication(RequestContext context, Asset asset) {
        final HostIdentifier hostIdentifier;

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

        final Clusters clusters = asset.findView(Clusters.class);

        if (clusters == null) {
            Jvm.warn().on(getClass(), "no cluster found name=" + context.cluster());
            return;
        }

        final EngineCluster engineCluster = clusters.get(context.cluster());
        final String csp = context.fullName();

        if (engineCluster == null) {
            Jvm.warn().on(getClass(), "no cluster found name=" + context.cluster());
            return;
        }

        byte localIdentifier = hostIdentifier.hostId();

        if (LOG.isDebugEnabled())
            Jvm.debug().on(getClass(), "hostDetails : localIdentifier=" + localIdentifier + ",cluster=" + engineCluster.hostDetails());

        // if true - each replication event sends back an enableAcknowledgment
        final boolean acknowledgement = queueConfig.acknowledgment();
        final MessageAdaptor messageAdaptor = queueConfig.bytesFunction();

        for (EngineHostDetails hostDetails : engineCluster.hostDetails()) {

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

                WriteMarshallable h = isSource0 ?

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

    @Override
    public void registerTopicSubscriber
            (@NotNull TopicSubscriber<T, M> topicSubscriber) throws AssetNotFoundException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void unregisterTopicSubscriber(@NotNull TopicSubscriber<T, M> topicSubscriber) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Publisher<M> publisher(@NotNull T topic) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void registerSubscriber(@NotNull T topic, @NotNull Subscriber<M> subscriber) {
        throw new UnsupportedOperationException("todo");
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

        File baseFilePath;
        if (basePath == null)
            baseFilePath = new File(defaultPath, "" + hostID);
        else
            baseFilePath = new File(basePath, name);

        if (!baseFilePath.exists())
            Files.createDirectories(baseFilePath.toPath());

        final SingleChronicleQueueBuilder builder = wireType == DEFAULT_ZERO_BINARY
                ? defaultZeroBinary(baseFilePath)
                : binary(baseFilePath);
        // TODO make configurable
//        builder.blockSize(256 << 20);

        return builder.build();
    }

    private ExcerptTailer threadLocalTailer() {
        return threadLocal.get().tailer;
    }

    private ExcerptAppender threadLocalAppender() {
        return threadLocal.get().appender;
    }

    public Tailer<T, M> tailer() {
        final ExcerptTailer tailer = ChronicleQueueView.this.chronicleQueue.createTailer();
        final LocalExcept localExcept = new LocalExcept();
        return () -> ChronicleQueueView.this.next(tailer, localExcept);
    }

    private Excerpt<T, M> next(ExcerptTailer excerptTailer, final LocalExcept excerpt) {
        excerpt.clear();
        try (DocumentContext dc = excerptTailer.readingDocument()) {
            if (!dc.isPresent())
                return null;
            final Wire wire = dc.wire();
            final T topic = wire.readEvent(messageTypeClass);
            final ValueIn valueIn = wire.getValueIn();
            if (Bytes.class.isAssignableFrom(elementTypeClass)) {
                valueIn.text(excerpt.text());
            } else {
                final M message = valueIn.object(elementTypeClass);
                excerpt.message(message);
            }
            return excerpt
                    .topic(topic)
                    .index(excerptTailer.index());
        }
    }

    /**
     * @param index gets the except at the given index
     * @return the except
     */
    @Nullable
    @Override
    public Excerpt<T, M> get(long index) {
        final ThreadLocalData threadLocalData = threadLocal.get();
        ExcerptTailer excerptTailer = threadLocalData.replayTailer;

        if (!excerptTailer.moveToIndex(index))
            return null;

        try (DocumentContext dc = excerptTailer.readingDocument()) {
            if (!dc.isPresent())
                return null;
            final StringBuilder topic = Wires.acquireStringBuilder();
            final M message = dc.wire().readEventName(topic).object(elementTypeClass);

            return threadLocalData.excerpt
                    .message(message)
                    .topic(convertTo(messageTypeClass, topic))
                    .index(excerptTailer.index());
        }
    }

    @Override
    public Excerpt<T, M> get(T topic) {

        final ThreadLocalData threadLocalData = threadLocal.get();
        ExcerptTailer excerptTailer = threadLocalData.replayTailer;
        for (; ; ) {

            try (DocumentContext dc = excerptTailer.readingDocument()) {
                if (!dc.isPresent())
                    return null;
                final StringBuilder t = Wires.acquireStringBuilder();
                final ValueIn valueIn = dc.wire().readEventName(t);

                final T topic1 = convertTo(messageTypeClass, t);

                if (!topic.equals(topic1))
                    continue;

                final M message = valueIn.object(elementTypeClass);

                return threadLocalData.excerpt
                        .message(message)
                        .topic(topic1)
                        .index(excerptTailer.index());
            }
        }
    }

    @Override
    public void publish(@NotNull T topic, @NotNull M message) {
        publishAndIndex(topic, message);
    }

    /**
     * @param consumer a consumer that provides that name of the event and value contained within
     *                 the except
     */
    public void get(@NotNull BiConsumer<CharSequence, M> consumer) {
        final ExcerptTailer tailer = threadLocalTailer();

        tailer.readDocument(w -> {
            final StringBuilder eventName = Wires.acquireStringBuilder();
            final ValueIn valueIn = w.readEventName(eventName);
            consumer.accept(eventName, valueIn.object(elementTypeClass));

        });
    }

    public long publishAndIndex(@NotNull T topic, @NotNull M message) {

        if (isReplicating && !isSource)
            throw new IllegalStateException("You can not publish to a sink used in replication, " +
                    "you have to publish to the source");

        final ExcerptAppender excerptAppender = threadLocalAppender();

        try (final DocumentContext dc = excerptAppender.writingDocument()) {
            dc.wire().writeEvent(messageTypeClass, topic).object(elementTypeClass, message);
        }
        return excerptAppender.lastIndexAppended();
    }

    public long set(@NotNull M event) {
        if (isReplicating && !isSource)
            throw new IllegalStateException("You can not publish to a sink used in replication, " +
                    "you have to publish to the source");
        final ExcerptAppender excerptAppender = threadLocalAppender();
        excerptAppender.writeDocument(w -> w.writeEventName(() -> "").object(event));
        return excerptAppender.lastIndexAppended();
    }

    public void clear() {
        chronicleQueue.clear();
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

        File file = chronicleQueue.file();
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

    public <M> void registerSubscriber(Subscriber<M> subscriber) {

    }

    public void unregisterSubscriber(Subscriber subscriber) {

    }

    public int subscriberCount() {
        throw new UnsupportedOperationException("todo");
    }

    public String dump() {
        return chronicleQueue.dump();
    }

    @Override
    public <E> Asset createSubAsset(VanillaAsset vanillaAsset, String name, Class<E> valueType) {
        return new VanillaSubAsset<>(vanillaAsset, name, valueType, null);
    }

    public static class LocalExcept<T, M> implements Excerpt<T, M>, Marshallable {
        private T topic;
        private M message;
        private Bytes bytes;
        private long index;

        @Override
        public T topic() {
            return topic;
        }

        @Override
        public M message() {
            return message;
        }

        @Override
        public long index() {
            return this.index;
        }

        public LocalExcept<T, M> index(long index) {
            this.index = index;
            return this;
        }

        LocalExcept message(M message) {
            this.message = message;
            return this;
        }

        LocalExcept topic(T topic) {
            this.topic = topic;
            return this;
        }

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
    }

    private class ThreadLocalData {

        final ExcerptAppender appender;
        final ExcerptTailer tailer;
        final ExcerptTailer replayTailer;
        final LocalExcept excerpt;

        ThreadLocalData(ChronicleQueue chronicleQueue) {
            appender = chronicleQueue.acquireAppender();
            appender.padToCacheAlign(net.openhft.chronicle.wire.MarshallableOut.Padding.ALWAYS);
            tailer = chronicleQueue.createTailer();

            replayTailer = chronicleQueue.createTailer();
            excerpt = new LocalExcept();
        }
    }
}

