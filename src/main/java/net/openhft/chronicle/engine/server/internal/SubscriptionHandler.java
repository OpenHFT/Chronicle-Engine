package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.Subscription;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.wire.ParameterizeWireKey;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireKey;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static net.openhft.chronicle.engine.server.internal.SubscriptionHandler.SubscriptionEventID.*;
import static net.openhft.chronicle.network.connection.CoreFields.reply;

/**
 * Created by rob on 28/06/2015.
 */
public class SubscriptionHandler<T extends Subscription> extends AbstractHandler {
    private static final int MAX_QUEUE_LENGTH = 10;

    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionHandler.class);

    final StringBuilder eventName = new StringBuilder();
    final Map<Long, Object> tidToListener = new ConcurrentHashMap<>();
    protected Wire outWire;
    protected T subscription;
    protected RequestContext requestContext;
    protected Queue<Consumer<Wire>> publisher;
    protected AssetTree assetTree;

    protected void publisherAdd(Consumer<Wire> toPublish) {
        if (publisher.size() > MAX_QUEUE_LENGTH)
            System.out.println("publish queue length: " + publisher.size());
        publisher.add(toPublish);
    }

    /**
     * after writing the tid to the wire
     *
     * @param eventName the name of the event
     * @return true if processed
     */
    protected boolean after(StringBuilder eventName) {

        if (topicSubscriberCount.contentEquals(eventName)) {
            outWire.writeEventName(reply).int8(subscription.topicSubscriberCount());
            return true;
        }

        if (keySubscriberCount.contentEquals(eventName)) {
            outWire.writeEventName(reply).int8(subscription.keySubscriberCount());
            return true;
        }

        if (entrySubscriberCount.contentEquals(eventName)) {
            outWire.writeEventName(reply).int8(subscription.entrySubscriberCount());
        }

        return false;
    }


    /**
     * before writing the tid to the wire
     *
     * @param tid     the tid
     * @param valueIn the value in from the wire
     * @return true if processed
     */
    protected boolean before(Long tid, ValueIn valueIn) throws AssetNotFoundException {
        if (registerSubscriber.contentEquals(eventName)) {
            Class subscriptionType = valueIn.typeLiteral();
            Subscriber<Object> listener = new LocalSubscriber(tid);
            tidToListener.put(tid, listener);
            RequestContext rc = requestContext.clone().type(subscriptionType);
            assetTree.acquireSubscription(rc).registerSubscriber(rc, listener);

            return true;
        }
        if (unRegisterSubscriber.contentEquals(eventName)) {
            Subscriber<Object> listener = (Subscriber) tidToListener.remove(tid);
            if (listener == null) {
                SubscriptionHandler.LOG.warn("No subscriber to present to unRegisterSubscriber (" + tid + ")");
                return true;
            }
            assetTree.unregisterSubscriber(requestContext.name(), listener);

            return true;
        }
        return false;
    }


    public enum SubscriptionEventID implements ParameterizeWireKey {

        registerSubscriber,
        unRegisterSubscriber,
        keySubscriberCount,
        entrySubscriberCount,
        topicSubscriberCount;

        private final WireKey[] params;

        <P extends WireKey> SubscriptionEventID(P... params) {
            this.params = params;
        }

        @NotNull
        public <P extends WireKey> P[] params() {
            return (P[]) this.params;
        }
    }

    class LocalSubscriber implements Subscriber<Object> {
        private final Long tid;

        LocalSubscriber(Long tid) {
            this.tid = tid;
        }

        @Override
        public void onMessage(Object e) throws InvalidSubscriberException {
            SubscriptionHandler.this.publisherAdd(publish -> {
                publish.writeDocument(true, wire -> wire.writeEventName(CoreFields.tid).int64(tid));
                publish.writeNotReadyDocument(false, wire -> wire.write(reply).object(e));
            });
        }

        @Override
        public void onEndOfSubscription() {
            // no more data.
            publisherAdd(publish -> {
                publish.writeDocument(true, wire -> wire.writeEventName(CoreFields.tid).int64(tid));
                publish.writeDocument(false, wire -> wire.write(reply).typedMarshallable(null));
            });
        }
    }
}
