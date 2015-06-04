package net.openhft.chronicle.engine.pubsub;

import net.openhft.chronicle.engine.api.*;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Supplier;

/**
 * Created by peter on 29/05/15.
 */
public class SimpleSubscription<E> implements Subscription {
    private final Set<Subscriber<E>> subscribers = new CopyOnWriteArraySet<>();
    private final Supplier<E> currentValue;

    public SimpleSubscription(Supplier<E> currentValue) {
        this.currentValue = currentValue;
    }

    @Override
    public boolean hasSubscribers() {
        return !subscribers.isEmpty();
    }

    @Override
    public <E> void registerSubscriber(RequestContext rc, Subscriber<E> subscriber) {
        subscribers.add((Subscriber) subscriber);
        if (rc.bootstrap() != Boolean.FALSE)
            try {
                subscriber.onMessage((E) currentValue.get());
            } catch (InvalidSubscriberException e) {
                subscribers.remove(subscriber);
            }
    }

    @Override
    public <T, E> void registerTopicSubscriber(RequestContext rc, TopicSubscriber<T, E> subscriber) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void unregisterSubscriber(RequestContext rc, Subscriber subscriber) {
        subscribers.remove(subscriber);
    }

    @Override
    public void unregisterTopicSubscriber(RequestContext rc, TopicSubscriber subscriber) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void registerDownstream(Subscription subscription) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void unregisterDownstream(Subscription subscription) {
        throw new UnsupportedOperationException("todo");
    }

    public void notifyMessage(E e) {
        SubscriptionConsumer.notifyEachSubscriber(subscribers, s -> s.onMessage(e));
    }

    @Override
    public boolean keyedView() {
        return false;
    }
}
