package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Publisher;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.server.internal.PublisherHandler.EventId;
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

import static net.openhft.chronicle.engine.server.internal.PublisherHandler.EventId.registerTopicSubscriber;

/**
 * Created by Rob Austin
 */
public class RemotePublisher<E> extends AbstractStatelessClient<EventId> implements Publisher<E> {


    private final Class<E> messageClass;

    public RemotePublisher(@NotNull RequestContext context, @NotNull Asset asset, Object underlying)
            throws AssetNotFoundException {
        super(asset.findView(TcpChannelHub.class), (long) 0, toUri(context));

        messageClass = context.messageType();
    }

    private static String toUri(@NotNull final RequestContext context) {
        StringBuilder uri = new StringBuilder( "/" + context.fullName()
                + "?view=" + "publisher");

        if (context.valueType() != String.class)
            uri.append("&messageType=").append(context.messageType().getName());

        return uri.toString();
    }

    @Override
    public void publish(final E event) {
        checkEvent(event);
        sendEventAsync(EventId.publish, valueOut -> valueOut.object(event));
    }

    @Override
    public void registerSubscriber(@NotNull final Subscriber subscriber) throws AssetNotFoundException {

        if (hub.outBytesLock().isHeldByCurrentThread())
            throw new IllegalStateException("Cannot view map while debugging");

        final AbstractAsyncSubscription asyncSubscription = new AbstractAsyncSubscription(hub, csp) {

            @Override
            public void onSubsribe(@NotNull final WireOut wireOut) {
                wireOut.writeEventName(registerTopicSubscriber).text("");
            }

            @Override
            public void onConsumer(@NotNull final WireIn w) {
                w.readDocument(null, d -> {
                    final StringBuilder eventname = Wires.acquireStringBuilder();
                    final ValueIn valueIn = d.readEventName(eventname);

                    if (EventId.onEndOfSubscription.contentEquals(eventname)) {
                        subscriber.onEndOfSubscription();
                        hub.unsubscribe(tid());
                    }else if (CoreFields.reply.contentEquals(eventname)) {
                        valueIn.marshallable(m -> {
                            final E message = m.read(() -> "message").object(messageClass);
                            RemotePublisher.this.onEvent(message, subscriber);
                        });
                    }

                });
            }

        };

        hub.subscribe(asyncSubscription);
    }

    private void onEvent(@Nullable E message, @NotNull Subscriber<E> subscriber) {
        try {
            if (message == null) {
                // todo
            } else
                subscriber.onMessage(message);
        } catch (InvalidSubscriberException noLongerValid) {
            // todo
        }
    }

    private void checkEvent(@Nullable Object key) {
        if (key == null)
            throw new NullPointerException("event can not be null");
    }
}
