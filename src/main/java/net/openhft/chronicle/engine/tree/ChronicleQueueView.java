package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.engine.api.pubsub.Publisher;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.RequestContext;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.BiConsumer;

/**
 * @author Rob Austin.
 */
public class ChronicleQueueView<T, M> implements QueueView<T, M> {

    private static final String DEFAULT_BASE_PATH;
    private static final Logger LOG = LoggerFactory.getLogger(ChronicleQueueView.class);

    static {
        String dir = "/tmp";
        try {
            final Path tempDirectory = Files.createTempDirectory("engine-queue");
            dir = tempDirectory.toAbsolutePath().toString();
        } catch (Exception ignore) {
        }

        DEFAULT_BASE_PATH = dir;
    }

    private final ChronicleQueue chronicleQueue;
    private final Class<T> messageTypeClass;
    @NotNull
    private final Class<M> elementTypeClass;
    private final ThreadLocal<ThreadLocalData> threadLocal;

    public ChronicleQueueView(RequestContext context, Asset asset) {
        chronicleQueue = newInstance(context.name(), context.basePath());
        messageTypeClass = context.messageType();
        elementTypeClass = context.elementType();
        threadLocal = ThreadLocal.withInitial(() -> new ThreadLocalData(chronicleQueue));

      /*  HostIdentifier hostIdentifier = null;
        boolean isSource = true;
        if (hostIdentifier != null) {
            Clusters clusters = asset.findView(Clusters.class);

            if (clusters == null) {
                LOG.warn("no clusters found.");
                return;
            }

            final Cluster cluster = clusters.get(context.cluster());

            if (cluster == null) {
                LOG.warn("no cluster found name=" + context.cluster());
                return;
            }

            byte localIdentifier = hostIdentifier.hostId();

            if (LOG.isDebugEnabled())
                LOG.debug("hostDetails : localIdentifier=" + localIdentifier + ",cluster=" + cluster.hostDetails());

            for (HostDetails hostDetails : cluster.hostDetails()) {
                try {
                    // its the identifier with the larger values that will establish the connection
                    byte remoteIdentifier = (byte) hostDetails.hostId;

                    if (remoteIdentifier <= localIdentifier) {

                        if (LOG.isDebugEnabled())
                            LOG.debug("skipping : attempting to connect to localIdentifier=" +
                                    localIdentifier + ", remoteIdentifier=" + remoteIdentifier);

                        continue;
                    }

                    if (LOG.isDebugEnabled())
                        LOG.debug("attempting to connect to localIdentifier=" + localIdentifier + ", " +
                                "remoteIdentifier=" + remoteIdentifier);

                    // the wire type used for replication

                    final WireType wireType = context.wireType();



                    final QueueReplicationHandler replicationHandler = new
                            QueueReplicationHandler(!isSource);



                    final QueueReplicationHandler replicationHandler = new
                            QueueReplicationHandler(!isSource);

                    final String csp = context.fullName() + "?view=" + "Replication";
                    final WriteMarshallable out = w -> {

                        w.writeDocument(true, m -> {
                            m.writeEventName(CoreFields.csp).text(csp);
                            m.writeEventName(CoreFields.cid).int64(uniqueCspId());
                            m.writeEventName(CoreFields.handler).typedMarshallable(replicationHandler);
                        });

                        w.writeDocument(false, d -> d.writeEventName(bootstrap)
                                .int64(lastUpdateTime).writeComment
                                        ("the-client:localIdentifier=" + localIdentifier + "," +
                                                "remoteIdentifier=" +
                                                remoteIdentifier + ",Thread=" + Thread.currentThread
                                                ().getName() + ",Thread=" + Thread.currentThread().getName()));
                    };

                    hostDetails.connect(
                            wireType,
                            asset,
                            w -> toHeader(replicationHandler, localIdentifier, remoteIdentifier)
                                    .writeMarshallable(w),
                            VanillaWireOutPublisher::new, out);

                    assert localIdentifier != remoteIdentifier : "remoteIdentifier=" + remoteIdentifier;

                } catch (Exception e) {
                    LOG.error("hostDetails=" + hostDetails, e);
                }
            }
        }*/


    }

    @NotNull
    public static String resourcesDir() {
        String path = ChronicleQueueView.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        if (path == null)
            return ".";
        return new File(path).getParentFile().getParentFile() + "/src/test/resources";
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

    private ChronicleQueue newInstance(String name, @Nullable String basePath) {
        ChronicleQueue chronicleQueue;

        if (basePath == null)
            basePath = DEFAULT_BASE_PATH;
        File baseFilePath;
        try {
            baseFilePath = new File(basePath, name);
            baseFilePath.mkdirs();
            chronicleQueue = new SingleChronicleQueueBuilder(baseFilePath).build();
        } catch (Exception e) {
            throw Jvm.rethrow(e);
        }
        return chronicleQueue;
    }


    private ExcerptTailer threadLocalTailer() {
        return threadLocal.get().tailer;
    }


    //  @Override
    public ExcerptAppender threadLocalAppender() {
        return threadLocal.get().appender;
    }

    @Override
    public Excerpt<T, M> next() {
        final ThreadLocalData threadLocalData = threadLocal.get();
        ExcerptTailer excerptTailer = threadLocalData.replayTailer;

        try (DocumentContext dc = excerptTailer.readingDocument()) {
            if (!dc.isPresent())
                return null;
            final StringBuilder topic = Wires.acquireStringBuilder();
            final ValueIn eventName = dc.wire().readEventName(topic);
            final M message = eventName.object(elementTypeClass);
            return threadLocalData.excerpt
                    .message(message)
                    .topic(ObjectUtils.convertTo(messageTypeClass, topic))
                    .index(excerptTailer.index());
        }

    }


    /**
     * @param index gets the except at the given index or {@code null} if the index is not valid
     * @return the except
     */
    @Nullable
    @Override
    public Excerpt<T, M> get(long index) {
        final ThreadLocalData threadLocalData = threadLocal.get();
        ExcerptTailer excerptTailer = threadLocalData.replayTailer;

        excerptTailer.moveToIndex(index);
        try (DocumentContext dc = excerptTailer.readingDocument()) {
            if (!dc.isPresent())
                return null;
            final StringBuilder topic = Wires.acquireStringBuilder();
            final M message = dc.wire().readEventName(topic).object(elementTypeClass);

            return threadLocalData.excerpt
                    .message(message)
                    .topic(ObjectUtils.convertTo(messageTypeClass, topic))
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
                final ValueIn eventName = dc.wire().readEventName(t);

                final T topic1 = ObjectUtils.convertTo(messageTypeClass, topic);

                if (!topic.equals(topic1))
                    continue;

                final M message = eventName.object(elementTypeClass);
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
        final WireKey wireKey = topic instanceof WireKey ? (WireKey) topic : topic::toString;
        final ExcerptAppender excerptAppender = threadLocalAppender();

        try (final DocumentContext dc = excerptAppender.writingDocument()) {
            dc.wire().writeEventName(wireKey).object(message);
            return excerptAppender.lastIndexAppended();
        }

    }


    public long set(@NotNull M event) {
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
        return chronicleQueue.lastIndex();
    }

    @NotNull
    public WireType wireType() {
        throw new UnsupportedOperationException("todo");
    }


    public void close() throws IOException {
        chronicleQueue.close();
    }

    public <M> void registerSubscriber(Subscriber<M> subscriber) {

    }

    public void unregisterSubscriber(Subscriber subscriber) {

    }

    public int subscriberCount() {
        throw new UnsupportedOperationException("todo");
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

