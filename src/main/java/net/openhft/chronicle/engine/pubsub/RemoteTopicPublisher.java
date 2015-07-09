package net.openhft.chronicle.engine.pubsub;

import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.TopicPublisher;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
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

    private final MapView<T, M, M> underlying;
    private final Class<T> topicClass;
    private final Class<M> messageClass;
    private Asset asset;


    public RemoteTopicPublisher(@NotNull RequestContext context, @NotNull Asset asset, MapView<T, M, M> underlying)
            throws AssetNotFoundException {
        super(asset.findView(TcpChannelHub.class), (long) 0, toUri(context));
        final RequestContext context1 = context;
        this.asset = asset;
        this.underlying = underlying;
        topicClass = context.topicType();
        messageClass = context.messageType();

    }

    private static String toUri(@NotNull final RequestContext context) {
        final StringBuilder uri = new StringBuilder("/" + context.fullName()
                + "?view=" + "topicPublisher");

        if (context.keyType() != String.class)
            uri.append("&topicType=").append(context.topicType().getName());

        if (context.valueType() != String.class)
            uri.append("&messageType=").append(context.messageType().getName());

        return uri.toString();
    }

    @Override
    public void publish(final T topic, final M message) {
        checkTopic(topic);
        checkMessage(message);
        sendEventAsync(publish, valueOut -> valueOut.marshallable(m -> {
            m.write(Params.topic).object(topic);
            m.write(Params.message).object(message);
        }));

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

        hub.subscribe(new AbstractAsyncSubscription(hub, csp) {

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
                            final T topic = m.read(() -> "message").object(topicClass);
                            final M message = m.read(() -> "message").object(messageClass);
                            RemoteTopicPublisher.this.onEvent(topic, message, topicSubscriber);
                        });
                    }
                });
            }


        });

    }

    @Override
    public void unregisterTopicSubscriber(TopicSubscriber<T, M> topicSubscriber) {
        // TODO CE-101
        throw new UnsupportedOperationException("todo");
    }

    private void onEvent(T topic, @Nullable M message, @NotNull TopicSubscriber<T, M> topicSubscriber) {
        try {
            if (message == null) {
                // todo
            } else
                topicSubscriber.onMessage(topic, message);
        } catch (InvalidSubscriberException noLongerValid) {
            // todo
        }
    }

    @Override
    public Asset asset() {
        return asset;
    }

    // todo not sure the interface for this is correct  ? should it be a map view ?
    @Override
    public MapView<T, M, M> underlying() {
        return underlying;
    }


}
