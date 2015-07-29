package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.Subscription;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static net.openhft.chronicle.engine.server.internal.SubscriptionHandler.SubscriptionEventID.*;
import static net.openhft.chronicle.network.connection.CoreFields.reply;

/**
 * Created by rob on 28/06/2015.
 */
public class SubscriptionHandler<T extends Subscription> extends AbstractHandler {
    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionHandler.class);

    final StringBuilder eventName = new StringBuilder();
    final Map<Long, Object> tidToListener = new ConcurrentHashMap<>();
    protected Wire outWire;
    protected T subscription;
    protected RequestContext requestContext;
    protected WireOutPublisher publisher;
    protected AssetTree assetTree;

    /**
     * after writing the tid to the wire
     *
     * @param eventName the name of the event
     * @return true if processed
     */
    protected boolean after(@NotNull StringBuilder eventName) {

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
    protected boolean before(Long tid, @NotNull ValueIn valueIn) throws AssetNotFoundException {
        if (registerSubscriber.contentEquals(eventName)) {
            Class subscriptionType = valueIn.typeLiteral();
            if(tidToListener.containsKey(tid)){
                System.out.println("Duplicate registration for tid " + tid);
                return true;
            }
            Subscriber<Object> listener = new LocalSubscriber(tid);
            tidToListener.put(tid, listener);
            RequestContext rc = requestContext.clone().type(subscriptionType);
            assetTree.acquireSubscription(rc).registerSubscriber(rc, listener);

            return true;
        }
        if (unregisterSubscriber.contentEquals(eventName)) {
            Subscriber<Object> listener = (Subscriber) tidToListener.remove(tid);
            if (listener == null) {
                SubscriptionHandler.LOG.warn("No subscriber to present to unregisterSubscriber (" + tid + ")");
                return true;
            }
            assetTree.unregisterSubscriber(requestContext.name(), listener);

            return true;
        }
        return false;
    }

    public enum SubscriptionEventID implements ParameterizeWireKey {

        registerSubscriber,
        unregisterSubscriber,
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
            Consumer<WireOut> toPublish = publish -> {
                publish.writeDocument(true, wire -> wire.writeEventName(CoreFields.tid).int64(tid));
                publish.writeNotReadyDocument(false, wire -> wire.write(reply).object(e));
            };
            publisher.add(toPublish);
        }

        @Override
        public void onEndOfSubscription() {
            if (!publisher.isClosed()) {
                // no more data.
                Consumer<WireOut> toPublish = publish -> {
                    publish.writeDocument(true, wire -> wire.writeEventName(CoreFields.tid).int64(tid));
                    publish.writeDocument(false, wire ->
                            wire.writeEventName(ObjectKVSubscriptionHandler.EventId.onEndOfSubscription).text(""));
                };
                publisher.add(toPublish);
            }
        }

        @NotNull
        @Override
        public String toString() {
            return "LocalSubscriber{" +
                    "tid=" + tid +
                    '}';
        }
    }
}
