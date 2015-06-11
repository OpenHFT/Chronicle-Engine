package net.openhft.chronicle.engine.api.pubsub;

import net.openhft.lang.Jvm;
import org.jetbrains.annotations.NotNull;

import java.util.Set;

/**
 * Created by peter on 02/06/15.
 */
@FunctionalInterface
public interface SubscriptionConsumer<T> {
    static <S extends ISubscriber> void notifyEachSubscriber(@NotNull Set<S> subs, @NotNull SubscriptionConsumer<S> doNotify) {
        doNotify.notifyEachSubscriber(subs);
    }

    default void notifyEachSubscriber(@NotNull Set<T> subs) {
        subs.forEach(s -> {
            try {
                accept(s);
            } catch (InvalidSubscriberException ise) {
                subs.remove(s);
            }
        });
    }

    static <E> void notifyEachEvent(@NotNull Set<E> subs, @NotNull SubscriptionConsumer<E> doNotify) throws InvalidSubscriberException {
        doNotify.notifyEachEvent(subs);
    }

    default void notifyEachEvent(@NotNull Set<T> subs) throws InvalidSubscriberException {
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
