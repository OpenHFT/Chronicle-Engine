package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.engine.api.pubsub.TopicPublisher;
import net.openhft.chronicle.engine.api.tree.KeyedView;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author Rob Austin.
 */
public interface QueueView<T, M> extends ChronicleQueue, TopicPublisher<T, M>, KeyedView {
    ExcerptTailer threadLocalTailer();

    ExcerptAppender threadLocalAppender();

    void threadLocalElement(M e);

    @Nullable
    M threadLocalElement();

    @Nullable
    M get(int index);

    /**
     * @param eventName {@code null} if you wish to receive all events, otherwise the name of the
     *                  event that you wish to receive
     * @return the element that is stored under then {@code eventName} or {@code null} if none can
     * be found
     */
    @Nullable
    M get(@Nullable String eventName);

    /**
     * @param consumer a consumer that provides that name of the event and value contained within
     *                 the except
     */
    void get(BiConsumer<CharSequence, M> consumer);

    /**
     * @param name    the type of the  except
     * @param message the except to add
     * @return the index of the new except added to the chronicle queue
     */
    long set(@NotNull T name, @NotNull M message);

    /**
     * @param except the except to add
     * @return the index of the new except added to the chronicle queue
     */
    long set(@NotNull M except) throws IOException;

    long size();

    @Override
    void clear();

    @Override
    void close() throws IOException;

    /**
     * returns a except at and index
     *
     * @param index    the location of the except
     * @param consumer then consumer for the except
     * @param isAbsent can be {@code null} if you don't wish to provide a isAbsent, otherwise this
     *                 consumer will get called no except can be found at this {@code index}, this
     *                 could occur if the {@code index} is in the future or the index is in the
     *                 passed and is not available, as the chronicle file may have been deleted.
     */
    void replay(long index, @NotNull BiConsumer<T, M> consumer, @Nullable Consumer<Exception> isAbsent);

    Class<T> messageType();

    Class<M> elementTypeClass();
}
