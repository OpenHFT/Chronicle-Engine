package net.openhft.chronicle.engine.pubsub;

import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.tree.ChronicleQueueView.LocalExcept;
import net.openhft.chronicle.engine.tree.QueueView;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.engine.server.internal.TopicPublisherHandler.EventId.*;

/**
 * @author Rob Austin.
 */
public class RemoteQueueView<T, M> extends RemoteTopicPublisher<T, M> implements QueueView<T, M> {

    final ThreadLocal<LocalExcept<T, M>> threadLocal = ThreadLocal.withInitial(LocalExcept::new);

    public RemoteQueueView(@NotNull RequestContext requestContext, @NotNull Asset asset) {
        super(requestContext, asset, "QueueView");
    }

    @Override
    public Excerpt<T, M> next() {
        //noinspection unchecked
        return proxyReturnWireTypedObject(next, threadLocal.get(), LocalExcept.class);
    }

    @Override
    public Excerpt<T, M> get(long index) {
        //noinspection unchecked
        return proxyReturnWireTypedObject(getNextAtIndex, threadLocal.get(), LocalExcept.class, index);
    }

    @Override
    public Excerpt<T, M> get(T topic) {
        //noinspection unchecked
        return proxyReturnWireTypedObject(getNextAtTopic, threadLocal.get(), LocalExcept.class, topic);
    }

    @Override
    public long publishAndIndex(@NotNull T topic, @NotNull M message) {
        return proxyReturnLongWithArgs(publishAndIndex, topic, message);
    }


}
