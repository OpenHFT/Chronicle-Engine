package net.openhft.chronicle.engine;

import net.openhft.chronicle.engine.api.pubsub.Publisher;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.tree.QueueView;
import net.openhft.chronicle.queue.Excerpt;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author Rob Austin.
 */
public class RemoteQueueView implements QueueView {

/*    @Override
    public ExcerptTailer threadLocalTailer() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public ExcerptAppender threadLocalAppender() {
        throw new UnsupportedOperationException("todo");
    }*/

    @Nullable
    @Override
    public Object get(int index) {
        throw new UnsupportedOperationException("todo");
    }

    @Nullable
    @Override
    public Object get(@Nullable String eventName) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public long set(@NotNull Object name, @NotNull Object message) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public long set(@NotNull Object except) throws IOException {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    @Override
    public Excerpt createExcerpt() {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    @Override
    public ExcerptTailer createTailer() throws IOException {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    @Override
    public ExcerptAppender createAppender() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public long firstIndex() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public long lastIndex() {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    @Override
    public WireType wireType() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    @Override
    public File path() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Class messageType() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Class elementTypeClass() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void replay(long index, @NotNull BiConsumer consumer, @Nullable Consumer isAbsent) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void get(BiConsumer consumer) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void publish(@NotNull Object topic, @NotNull Object message) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void registerTopicSubscriber(@NotNull TopicSubscriber topicSubscriber) throws AssetNotFoundException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void unregisterTopicSubscriber(@NotNull TopicSubscriber topicSubscriber) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Publisher publisher(@NotNull Object topic) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void registerSubscriber(@NotNull Object topic, @NotNull Subscriber subscriber) {
        throw new UnsupportedOperationException("todo");
    }
}
