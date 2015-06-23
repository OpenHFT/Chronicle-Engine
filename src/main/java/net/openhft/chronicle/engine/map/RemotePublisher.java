package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Publisher;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.server.internal.PublisherHandler.EventId;
import net.openhft.chronicle.network.connection.AbstractStatelessClient;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.ValueIn;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static net.openhft.chronicle.engine.server.internal.PublisherHandler.EventId.registerTopicSubscriber;
import static net.openhft.chronicle.network.connection.CoreFields.reply;

/**
 * Created by Rob Austin
 */
public class RemotePublisher<E> extends AbstractStatelessClient<EventId> implements Publisher<E> {

    private final RequestContext context;

    public RemotePublisher(@NotNull RequestContext context, Asset asset, Object underlying)
            throws AssetNotFoundException {
        super(asset.findView(TcpChannelHub.class), (long) 0, toUri(context));
        this.context = context;
    }

    private static String toUri(final RequestContext context) {
        StringBuilder uri = new StringBuilder("/" + context.name()
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
    public void registerSubscriber(final Subscriber subscriber) throws AssetNotFoundException {
        final long startTime = System.currentTimeMillis();

        if (hub.outBytesLock().isHeldByCurrentThread())
            throw new IllegalStateException("Cannot view map while debugging");

        hub.outBytesLock().lock();
        try {
            long tid = writeMetaDataStartTime(startTime);

            hub.outWire().writeDocument(false, wireOut ->
                    wireOut.writeEventName(registerTopicSubscriber).text(""));

            hub.asyncReadSocket(tid, w -> w.readDocument(null, d -> {
                ValueIn valueIn = d.read(reply);
                valueIn.marshallable(m -> {
                    final E message = (E) m.read(() -> "message").object(context.messageType());
                    this.onEvent(message, subscriber);
                });
            }));
            hub.writeSocket(hub.outWire());
        } finally {
            hub.outBytesLock().unlock();
        }

    }

    private void onEvent(E message, Subscriber<E> subscriber) {
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
