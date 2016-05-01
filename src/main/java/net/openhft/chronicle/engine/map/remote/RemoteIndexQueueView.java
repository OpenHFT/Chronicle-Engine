package net.openhft.chronicle.engine.map.remote;

import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.query.IndexQuery;
import net.openhft.chronicle.engine.api.query.IndexQueueView;
import net.openhft.chronicle.engine.api.query.IndexedValue;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.server.internal.MapWireHandler;
import net.openhft.chronicle.network.connection.AbstractAsyncSubscription;
import net.openhft.chronicle.network.connection.AbstractStatelessClient;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.openhft.chronicle.engine.server.internal.IndexQueueViewHandler.EventId.*;
import static net.openhft.chronicle.network.connection.CoreFields.reply;

/**
 * @author Rob Austin.
 */
public class RemoteIndexQueueView<K extends Marshallable, V extends Marshallable> extends
        AbstractStatelessClient<MapWireHandler.EventId>
        implements IndexQueueView<Subscriber<IndexedValue<V>>, V> {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteIndexQueueView.class);
    private final Map<Object, Long> subscribersToTid = new ConcurrentHashMap<>();
    int i;


    public RemoteIndexQueueView(@NotNull final RequestContext context,
                                @NotNull Asset asset) {
        super(asset.findView(TcpChannelHub.class), (long) 0, toUri(context));
    }

    private static String toUri(@NotNull final RequestContext context) {
        return context.viewType(IndexQueueView.class).toUri();
    }

    @Override
    public void registerSubscriber(@NotNull Subscriber<IndexedValue<V>> subscriber, @NotNull IndexQuery<V> vanillaIndexQuery) {

        final AtomicBoolean hasAlreadySubscribed = new AtomicBoolean();

        if (hub.outBytesLock().isHeldByCurrentThread())
            throw new IllegalStateException("Cannot view map while debugging");

        final AbstractAsyncSubscription asyncSubscription = new AbstractAsyncSubscription(
                hub,
                csp,
                "RemoteIndexQueueView registerTopicSubscriber") {

            // this allows us to resubscribe from the last index we received
            volatile long fromIndex = 0;

            @Override
            public void onSubscribe(@NotNull final WireOut wireOut) {

                // this allows us to resubscribe from the last index we received
                if (hasAlreadySubscribed.getAndSet(true))
                    vanillaIndexQuery.fromIndex(fromIndex);

                subscribersToTid.put(subscriber, tid());
                wireOut.writeEventName(registerSubscriber)
                        .typedMarshallable(vanillaIndexQuery);
            }

            @Override
            public void onConsumer(@NotNull final WireIn inWire) {

                try (DocumentContext dc = inWire.readingDocument()) {
                    if (!dc.isPresent())
                        return;

                    StringBuilder sb = Wires.acquireStringBuilder();
                    ValueIn valueIn = dc.wire().readEventName(sb);

                    if (reply.contentEquals(sb))
                        try {
                            final IndexedValue<V> e = valueIn.typedMarshallable();
                            fromIndex = e.index();
                            subscriber.onMessage(e);
                        } catch (InvalidSubscriberException e) {
                            RemoteIndexQueueView.this.unregisterSubscriber(subscriber);
                        }
                    else if (onEndOfSubscription.contentEquals(sb)) {
                        subscriber.onEndOfSubscription();
                        hub.unsubscribe(tid());
                    }
                } catch (Exception e) {
                    LOG.error("", e);
                }

            }
        };

        hub.subscribe(asyncSubscription);

    }


    @Override
    public void unregisterSubscriber(@NotNull Subscriber<IndexedValue<V>> listener) {
        Long tid = subscribersToTid.get(listener);

        if (tid == null) {
            LOG.warn("There is no subscription to unsubscribe, was " + subscribersToTid.size() + " other subscriptions.");
            return;
        }

        hub.preventSubscribeUponReconnect(tid);

        if (!hub.isOpen()) {
            hub.unsubscribe(tid);
            return;
        }

        hub.lock(() -> {
            writeMetaDataForKnownTID(tid);
            hub.outWire().writeDocument(false, wireOut -> {
                wireOut.writeEventName(unregisterSubscriber).text("");
            });
        });

    }


}
