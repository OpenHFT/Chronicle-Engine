package net.openhft.chronicle.engine.pubsub;

import net.openhft.chronicle.engine.api.pubsub.*;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.server.internal.TopicPublisherHandler.EventId;
import net.openhft.chronicle.engine.server.internal.TopicPublisherHandler.Params;
import net.openhft.chronicle.network.connection.AbstractAsyncSubscription;
import net.openhft.chronicle.network.connection.AbstractStatelessClient;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static net.openhft.chronicle.engine.server.internal.PublisherHandler.EventId.registerSubscriber;
import static net.openhft.chronicle.engine.server.internal.TopicPublisherHandler.EventId.onEndOfSubscription;
import static net.openhft.chronicle.engine.server.internal.TopicPublisherHandler.EventId.publish;

/**
 * Created by Rob Austin
 */
public class RemoteTopicPublisher<T, M> extends AbstractStatelessClient<EventId> implements
        TopicPublisher<T, M> {

    final Class<T> topicClass;
    final Class<M> messageClass;
    private final Asset asset;

    public RemoteTopicPublisher(@NotNull RequestContext context, @NotNull Asset asset)
            throws AssetNotFoundException {
        super(asset.findView(TcpChannelHub.class), (long) 0, toUri(context, "topicPublisher"));
        this.asset = asset;
        topicClass = context.topicType();
        messageClass = context.messageType();

    }

    protected RemoteTopicPublisher(@NotNull RequestContext context, @NotNull Asset asset, String view)
            throws AssetNotFoundException {
        super(asset.findView(TcpChannelHub.class), (long) 0, toUri(context, view));
        this.asset = asset;
        topicClass = context.topicType();
        messageClass = context.messageType();

    }


    private static String toUri(@NotNull final RequestContext context, String view) {
        final StringBuilder uri = new StringBuilder("/" + context.fullName()
                + "?view=" + view);

        if (context.keyType() != String.class)
            uri.append("&topicType=").append(context.topicType().getName());

        if (context.valueType() != String.class)
            uri.append("&messageType=").append(context.messageType().getName());

        return uri.toString();
    }

    @Override
    public void publish(@NotNull final T topic, @NotNull final M message) {
        checkTopic(topic);
        checkMessage(message);
        sendEventAsync(publish, valueOut -> valueOut.marshallable(m -> {
            m.write(Params.topic).object(topic);
            m.write(Params.message).object(message);
        }), true);
    }

    private void checkTopic(@Nullable Object topic) {
        if (topic == null)
            throw new NullPointerException("topic can not be null");
    }

    private void checkMessage(@Nullable Object message) {
        if (message == null)
            throw new NullPointerException("message can not be null");
    }

    @Override
    public void registerTopicSubscriber(@NotNull final TopicSubscriber<T, M> topicSubscriber) throws
            AssetNotFoundException {

        if (hub.outBytesLock().isHeldByCurrentThread())
            throw new IllegalStateException("Cannot view map while debugging");

        hub.subscribe(new AbstractAsyncSubscription(hub, csp, "Remote Topic publisher register subscribe") {

            @Override
            public void onSubscribe(@NotNull final WireOut wireOut) {
                wireOut.writeEventName(registerSubscriber).text("");
            }

            @Override
            public void onConsumer(@NotNull final WireIn w) {
                w.readDocument(null, d -> {

                    final StringBuilder eventname = Wires.acquireStringBuilder();
                    final ValueIn valueIn = d.readEventName(eventname);

                    if (onEndOfSubscription.contentEquals(eventname)) {
                        topicSubscriber.onEndOfSubscription();
                        hub.unsubscribe(tid());
                    } else if (CoreFields.reply.contentEquals(eventname)) {
                        valueIn.marshallable(m -> {
                            final T topic = m.read(() -> "topic").object(topicClass);
                            final M message = m.read(() -> "message").object(messageClass);
                            RemoteTopicPublisher.this.onEvent(topic, message, topicSubscriber);
                        });
                    }
                });
            }
        });
    }

    @Override
    public void unregisterTopicSubscriber(@NotNull TopicSubscriber<T, M> topicSubscriber) {
        // TODO CE-101
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Publisher<M> publisher(@NotNull T topic) {
        throw new UnsupportedOperationException("tood");
    }

    @Override
    public void registerSubscriber(@NotNull T topic, @NotNull Subscriber<M> subscriber) {

    }

    private void onEvent(T topic, @Nullable M message, @NotNull TopicSubscriber<T, M> topicSubscriber) {
        try {
            if (message != null) {
                topicSubscriber.onMessage(topic, message);
            } else {
                // todo
            }
        } catch (InvalidSubscriberException noLongerValid) {
            // todo
        }
    }

}
