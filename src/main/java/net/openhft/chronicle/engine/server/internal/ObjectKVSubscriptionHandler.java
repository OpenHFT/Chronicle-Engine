package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.engine.api.pubsub.Subscription;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.map.ObjectKVSSubscription;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;

import static net.openhft.chronicle.engine.server.internal.ObjectKVSubscriptionHandler.EventId.registerTopicSubscriber;
import static net.openhft.chronicle.network.connection.CoreFields.reply;
import static net.openhft.chronicle.network.connection.CoreFields.tid;

/**
 * Created by Rob Austin
 */
public class ObjectKVSubscriptionHandler extends SubscriptionHandler<Subscription> {
    private static final Logger LOG = LoggerFactory.getLogger(ObjectKVSubscriptionHandler.class);

    @Nullable
    private final BiConsumer<WireIn, Long> dataConsumer = (inWire, inputTid) -> {

        eventName.setLength(0);
        final ValueIn valueIn = inWire.readEventName(eventName);

        if (registerTopicSubscriber.contentEquals(eventName)) {
            if (tidToListener.containsKey(tid)) {
                LOG.info("Duplicate topic registration for tid " + tid);
                return;
            }

            final TopicSubscriber listener = new TopicSubscriber() {
                volatile boolean subscriptionEnded;

                @Override
                public void onMessage(final Object topic, final Object message) {
                    assert !subscriptionEnded : "we received this message after the " +
                            "subscription has ended " + message;
                    WriteMarshallable toPublish = publish -> {
                        publish.writeDocument(true, wire -> wire.writeEventName(tid).int64(inputTid));
                        publish.writeNotReadyDocument(false, wire -> wire.writeEventName(reply)
                                .marshallable(m -> {
                                    m.write(() -> "topic").object(topic);
                                    m.write(() -> "message").object(message);
                                }));
                    };
                    publisher.add(toPublish);
                }

                public void onEndOfSubscription() {
                    subscriptionEnded = true;
                    if (!publisher.isClosed()) {
                        publisher.add(publish -> {
                            publish.writeDocument(true, wire ->
                                    wire.writeEventName(tid).int64(inputTid));
                            publish.writeDocument(false, wire ->
                                    wire.writeEventName(EventId.onEndOfSubscription).text(""));
                        });
                    }
                }
            };

            valueIn.marshallable(m -> {
                final Class kClass = m.read(() -> "keyType").typeLiteral();
                final Class vClass = m.read(() -> "valueType").typeLiteral();

                tidToListener.put(inputTid, listener);
                assetTree.registerTopicSubscriber(requestContext.fullName(), kClass, vClass, listener);
            });
            return;
        }

        if (EventId.unregisterTopicSubscriber.contentEquals(eventName)) {
            TopicSubscriber listener = (TopicSubscriber) tidToListener.remove(inputTid);
            if (listener == null) {
                LOG.warn("No subscriber to present to unsubscribe (" + inputTid + ")");
                return;
            }
            assetTree.unregisterTopicSubscriber(requestContext.name(), listener);

            return;
        }

        if (before(inputTid, valueIn)) return;

        outWire.writeDocument(true, wire -> outWire.writeEventName(tid).int64(inputTid));

        writeData(inWire.bytes(), out -> {

            if (after(eventName)) return;

            if (EventId.notifyEvent.contentEquals(eventName)) {
                ((ObjectKVSSubscription) subscription).notifyEvent(valueIn.typedMarshallable());
                outWire.writeEventName(reply).int8(subscription.entrySubscriberCount());
            }

        });

    };

    public ObjectKVSubscriptionHandler(final Throttler throttler) {
        super(throttler);
    }

    void process(@NotNull final WireIn inWire,
                 final RequestContext requestContext,
                 final WireOutPublisher publisher,
                 final AssetTree assetTree, final long tid,
                 final Wire outWire,
                 final Subscription subscription) {
        setOutWire(outWire);
        this.outWire = outWire;
        this.subscription = subscription;
        this.requestContext = requestContext;
        this.publisher = publisher;
        this.assetTree = assetTree;
        dataConsumer.accept(inWire, tid);

    }

    public enum EventId implements ParameterizeWireKey {
        registerTopicSubscriber,
        unregisterTopicSubscriber,
        onEndOfSubscription,
        notifyEvent;

        private final WireKey[] params;

        <P extends WireKey> EventId(P... params) {
            this.params = params;
        }

        @NotNull
        public <P extends WireKey> P[] params() {
            return (P[]) this.params;
        }
    }

}
