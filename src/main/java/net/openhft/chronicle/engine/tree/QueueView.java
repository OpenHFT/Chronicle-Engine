package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.engine.api.pubsub.TopicPublisher;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * @author Rob Austin.
 */
public interface QueueView<T, M> extends TopicPublisher<T, M> {


    interface Excerpt<T, M> {
        T topic();

        M message();

        long index();
    }

    /**
     * @return the next message from the current tailer
     */
    @Nullable
    Excerpt<T, M> next();

    /**
     * returns a {@link Excerpt} at a given index
     *
     * @param index the location of the except
     */
    @Nullable
    Excerpt<T, M> get(long index);

    /**
     * the next message from the current tailer which has this {@code topic}
     *
     * @param topic next excerpt that has this topic
     * @return the except
     */
    Excerpt<T, M> get(T topic);

    /**
     * Publish to a provided topic.
     *
     * @param topic   to publish to
     * @param message to publish.
     * @return the index in the chroncile queue the ex
     */
    long publishAndIndex(@NotNull T topic, @NotNull M message);

}
