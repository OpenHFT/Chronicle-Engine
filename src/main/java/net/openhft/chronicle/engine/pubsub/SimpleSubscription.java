package net.openhft.chronicle.engine.pubsub;

import net.openhft.chronicle.engine.api.map.ValueReader;
import net.openhft.chronicle.engine.api.pubsub.*;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import org.jetbrains.annotations.NotNull;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Created by peter on 29/05/15.
 */
public class SimpleSubscription<E> implements Subscription<E> {
    private final Set<Subscriber<E>> subscribers = new CopyOnWriteArraySet<>();
    private final Reference<E> currentValue;
    private final ValueReader<Object, E> valueReader;

    public SimpleSubscription(Reference<E> reference, ValueReader<Object, E> valueReader) {
        this.currentValue = reference;
        this.valueReader = valueReader;
    }

    @Override
    public void registerSubscriber(@NotNull RequestContext rc, @NotNull Subscriber<E> subscriber) {
        subscribers.add(subscriber);
        if (rc.bootstrap() != Boolean.FALSE)
            try {
                subscriber.onMessage(currentValue.get());
            } catch (InvalidSubscriberException e) {
                subscribers.remove(subscriber);
            }
    }

    @Override
    public void unregisterSubscriber(Subscriber<E> subscriber) {
        subscribers.remove(subscriber);
    }

    public void notifyMessage(Object e) {
        try {
            SubscriptionConsumer.notifyEachSubscriber(subscribers, s -> s.onMessage(valueReader.readFrom(e, null)));
        } catch (ClassCastException e1) {
            System.err.println("Is " + valueReader + " the correct ValueReader?");
            throw e1;
        }
    }

    @Override
    public boolean keyedView() {
        return false;
    }
}
