package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.map.ObjectKVSSubscription;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static net.openhft.chronicle.engine.server.internal.ObjectKVSubscriptionHandler.EventId.registerTopicSubscriber;
import static net.openhft.chronicle.network.connection.CoreFields.reply;
import static net.openhft.chronicle.network.connection.CoreFields.tid;

/**
 * Created by Rob Austin
 */
public class ObjectKVSubscriptionHandler extends SubscriptionHandler<ObjectKVSSubscription> {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectKVSubscriptionHandler.class);

    @Nullable
    private final BiConsumer<WireIn, Long> dataConsumer = (inWire, inputTid) -> {

        eventName.setLength(0);
        final ValueIn valueIn = inWire.readEventName(eventName);

        if (registerTopicSubscriber.contentEquals(eventName)) {

            final TopicSubscriber listener = new TopicSubscriber() {

                @Override
                public void onMessage(final Object topic, final Object message) throws InvalidSubscriberException {

                    publisher.add(publish -> {
                        publish.writeDocument(true, wire -> wire.writeEventName(tid).int64
                                (inputTid));
                        publish.writeNotReadyDocument(false, wire -> wire.write(reply)
                                .marshallable(m -> {
                                    m.write(() -> "topic").object(topic);
                                    m.write(() -> "message").object(message);
                                }));
                    });
                }

                public void onEndOfSubscription() {
                    publisher.add(publish -> {
                        publish.writeDocument(true, wire -> wire.writeEventName(tid).int64
                                (inputTid));
                        publish.writeNotReadyDocument(false, wire -> wire.writeEventName
                                (EventId.onEndOfSubscription).text(""));

                    });
                }
            };

            valueIn.marshallable(m -> {
                final Class kClass = m.read(() -> "keyType").typeLiteral();
                final Class vClass = m.read(() -> "valueType").typeLiteral();

                tidToListener.put(inputTid, listener);
                assetTree.registerTopicSubscriber(requestContext.name(), kClass, vClass, listener);
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
            // no more data.
            publisher.add(publish -> {
                publish.writeDocument(true, wire -> wire.writeEventName(tid).int64(inputTid));
                publish.writeDocument(false, wire -> wire.write(reply).typedMarshallable(null));
            });

            return;
        }

        if (before(inputTid, valueIn)) return;

        outWire.writeDocument(true, wire -> outWire.writeEventName(tid).int64(inputTid));

        writeData(inWire.bytes(), out -> {

            if (after(eventName)) return;

            if (EventId.notifyEvent.contentEquals(eventName)) {
                subscription.notifyEvent(valueIn.typedMarshallable());
                outWire.writeEventName(reply).int8(subscription.entrySubscriberCount());
            }

        });

    };

    void process(@NotNull final WireIn inWire,
                 final RequestContext requestContext,
                 final Queue<Consumer<Wire>> publisher,
                 final AssetTree assetTree, final long tid,
                 final Wire outWire,
                 final ObjectKVSSubscription subscription) {
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
