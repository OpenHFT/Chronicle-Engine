package net.openhft.chronicle.engine.api;

import net.openhft.lang.Jvm;

import java.util.Set;

/**
 * Created by peter on 02/06/15.
 */
@FunctionalInterface
public interface SubscriptionConsumer<T> {
    static <S extends ISubscriber> void notifyEachSubscriber(Set<S> subs, SubscriptionConsumer<S> doNotify) {
        doNotify.notifyEachSubscriber(subs);
    }

    default void notifyEachSubscriber(Set<T> subs) {
        subs.forEach(s -> {
            try {
                accept(s);
            } catch (InvalidSubscriberException ise) {
                subs.remove(s);
            }
        });
    }

    static <E> void notifyEachEvent(Set<E> subs, SubscriptionConsumer<E> doNotify) throws InvalidSubscriberException {
        doNotify.notifyEachEvent(subs);
    }

    default void notifyEachEvent(Set<T> subs) throws InvalidSubscriberException {
        subs.forEach(s -> {
            try {
                accept(s);
            } catch (InvalidSubscriberException e) {
                throw Jvm.rethrow(e);
            }
        });
    }

    void accept(T subscriber) throws InvalidSubscriberException;
}
