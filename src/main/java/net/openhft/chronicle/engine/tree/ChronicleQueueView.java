/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
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
import net.openhft.chronicle.engine.query.QueueSource;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
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

/**
 * @author Rob Austin.
 */
public class ChronicleQueueView<T, M> implements QueueView<T, M>, SubAssetFactory, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ChronicleQueueView.class);

    private final ChronicleQueue chronicleQueue;
    private final Class<T> messageTypeClass;
    @NotNull
    private final Class<M> elementTypeClass;
    private final ThreadLocal<ThreadLocalData> threadLocal;
    private final String defaultPath;
    private final RequestContext context;

    private boolean isSource;
    private boolean isReplicating;
    private boolean dontPersist;

    public ChronicleQueueView(@NotNull RequestContext context, @NotNull Asset asset) {
        this(null, context, asset);
    }

    public ChronicleQueueView(@Nullable ChronicleQueue queue,
                              @NotNull RequestContext context,
                              @NotNull Asset asset) {
        this.context = context;
        String s = asset.fullName();
        if (s.startsWith("/")) s = s.substring(1);
        defaultPath = s;
        final HostIdentifier hostIdentifier = asset.findOrCreateView(HostIdentifier.class);
        final Byte hostId = hostIdentifier == null ? null : hostIdentifier.hostId();
        chronicleQueue = queue != null ? queue : newInstance(context.name(), context.basePath(), hostId);
        messageTypeClass = context.messageType();
        elementTypeClass = context.elementType();
        threadLocal = ThreadLocal.withInitial(() -> new ThreadLocalData(chronicleQueue));
        dontPersist = context.dontPersist();

        if (hostId != null)
            replication(context, asset);
    }

    @NotNull
    public static String resourcesDir() {
        String path = ChronicleQueueView.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        if (path == null)
            return ".";
        return new File(path).getParentFile().getParentFile() + "/src/test/resources";
    }

    public static WriteMarshallable newSource(long nextIndexRequired, Class topicType, Class elementType, boolean acknowledgement) {
        try {
            Class<?> aClass = Class.forName("software.chronicle.enterprise.queue.QueueSourceReplicationHandler");
            Constructor<?> declaredConstructor = aClass.getDeclaredConstructor(long.class, Class.class,
                    Class.class, boolean.class);
            return (WriteMarshallable) declaredConstructor.newInstance(nextIndexRequired,
                    topicType, elementType, acknowledgement);
        } catch (Exception e) {
            IllegalStateException licence = new IllegalStateException("A Chronicle Queue Enterprise licence is" +
                    " required to run this code. Please contact sales@chronicle.software");
            LOG.error("", e);
            throw licence;
        }
    }

    public static WriteMarshallable newSync(Class topicType, Class elementType, boolean
            acknowledgement) {
        try {
            Class<?> aClass = Class.forName("software.chronicle.enterprise.queue.QueueSyncReplicationHandler");
            Constructor<?> declaredConstructor = aClass.getConstructor(Class.class, Class.class, boolean.class);
            return (WriteMarshallable) declaredConstructor.newInstance(topicType, elementType, acknowledgement);
        } catch (Exception e) {
            IllegalStateException licence = new IllegalStateException("A Chronicle Queue Enterprise licence is" +
                    " required to run this code." +
                    "Please contact sales@chronicle.software");
            LOG.error("", e);
            throw licence;
        }
    }

    public static void deleteFiles(File element) throws IOException {
        if (element.isDirectory()) {
            for (File sub : element.listFiles()) {
                deleteFiles(sub);
            }
        }
        try {
            Files.deleteIfExists(element.toPath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public ChronicleQueue chronicleQueue() {
        return chronicleQueue;
    }

    public void replication(RequestContext context, Asset asset) {

        final QueueSource queueSource;
        final HostIdentifier hostIdentifier;

        try {
            hostIdentifier = asset.findOrCreateView(HostIdentifier.class);
            queueSource = asset.findView(QueueSource.class);
        } catch (AssetNotFoundException anfe) {
            if (LOG.isDebugEnabled())
                LOG.debug("replication not enabled " + anfe.getMessage());
            return;
        }

        final int remoteSourceIdentifier = queueSource.sourceHostId(context.fullName());
        isSource = hostIdentifier.hostId() == remoteSourceIdentifier;

        isReplicating = true;

        final Clusters clusters = asset.findView(Clusters.class);

        if (clusters == null) {
            LOG.warn("no cluster found name=" + context.cluster());
            return;
        }
        final EngineCluster engineCluster = clusters.get(context.cluster());

        final String csp = context.fullName();

        if (engineCluster == null) {
            LOG.warn("no cluster found name=" + context.cluster());
            return;
        }

        byte localIdentifier = hostIdentifier.hostId();

        if (LOG.isDebugEnabled())
            LOG.debug("hostDetails : localIdentifier=" + localIdentifier + ",cluster=" + engineCluster.hostDetails());

        // if true - each replication event sends back an enableAcknowledgment
        boolean acknowledgement = queueSource.acknowledgment();

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
                        newSource(nextIndexRequired(), context.topicType(), context.elementType(), acknowledgement) :
                        newSync(context.topicType(), context.elementType(), acknowledgement);

                long cid = nc.newCid();
                nc.wireOutPublisher().publish(w -> w.writeDocument(true, d ->
                        d.writeEventName(CoreFields.csp).text(csp)
                                .writeEventName(CoreFields.cid).int64(cid)
                                .writeEventName(CoreFields.handler).typedMarshallable(h)));

            });
        }
    }

    private long nextIndexRequired() {
        try {
            final long index = chronicleQueue().nextIndexToWrite();
            // replay the last event just in case it was corrupted.
            return index - 1;
        } catch (IllegalStateException ignore) {
            return -1;
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

    private ChronicleQueue newInstance(String name, @Nullable String basePath, @Nullable Byte hostID) {
        ChronicleQueue chronicleQueue;

        File baseFilePath;
        if (basePath == null)
            baseFilePath = new File(defaultPath, "" + hostID);
        else
            baseFilePath = new File(basePath, name);
        try {
            if (!baseFilePath.exists())
                Files.createDirectories(baseFilePath.toPath());

            SingleChronicleQueueBuilder singleChronicleQueueBuilder = new SingleChronicleQueueBuilder(baseFilePath);

            if (context.wireType() != null)
                singleChronicleQueueBuilder.wireType(context.wireType());

            chronicleQueue = singleChronicleQueueBuilder.build();

        } catch (Exception e) {
            throw Jvm.rethrow(e);
        }
        return chronicleQueue;
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
            final StringBuilder topic = Wires.acquireStringBuilder();
            final ValueIn eventName = dc.wire().readEventName(topic);
            final M message = eventName.object(elementTypeClass);

            return excerpt
                    .message(message)
                    .topic(convertTo(messageTypeClass, topic))
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
        try {
            final ExcerptTailer tailer = threadLocalTailer();

            tailer.readDocument(w -> {
                final StringBuilder eventName = Wires.acquireStringBuilder();
                final ValueIn valueIn = w.readEventName(eventName);
                consumer.accept(eventName, valueIn.object(elementTypeClass));

            });
        } catch (Exception e) {
            e.printStackTrace();
            throw Jvm.rethrow(e);
        }
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

    public long firstIndex() {
        return chronicleQueue.firstIndex();
    }

    public long lastIndex() {
        return chronicleQueue.nextIndexToWrite();
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
                LOG.error("", e);
            }
        }


    }

    private void deleteFiles(TopicPublisher p) {

        if (p instanceof QueueTopicPublisher)
            deleteFiles((ChronicleQueueView) ((QueueTopicPublisher) p).underlying());

    }


    @Override
    protected void finalize() throws Throwable {
        try {
            close();
        } catch (Throwable ignore) {

        }
        super.finalize();

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
    }

    class ThreadLocalData {

        final ExcerptAppender appender;
        final ExcerptTailer tailer;
        final ExcerptTailer replayTailer;
        final LocalExcept excerpt;

        public ThreadLocalData(ChronicleQueue chronicleQueue) {
            appender = chronicleQueue.createAppender();
            tailer = chronicleQueue.createTailer();

            replayTailer = chronicleQueue.createTailer();
            excerpt = new LocalExcept();
        }
    }
}

