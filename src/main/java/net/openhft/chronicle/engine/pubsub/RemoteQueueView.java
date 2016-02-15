package net.openhft.chronicle.engine.pubsub;

import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.tree.QueueView;
import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
public class RemoteQueueView<T, M> extends RemoteTopicPublisher<T, M> implements QueueView<T, M> {

    public RemoteQueueView(@NotNull RequestContext requestContext, @NotNull Asset asset) {
        super(requestContext, asset, "QueueView");
    }

    @Override
    public Excerpt<T, M> next() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Excerpt<T, M> get(long index) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Excerpt<T, M> get(T topic) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public long publishAndIndex(@NotNull T topic, @NotNull M message) {
        throw new UnsupportedOperationException("todo");
    }


}
