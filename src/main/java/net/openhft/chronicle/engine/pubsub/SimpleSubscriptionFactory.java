package net.openhft.chronicle.engine.pubsub;

import net.openhft.chronicle.engine.api.pubsub.Reference;

import java.util.function.Function;

/**
 * Created by peter.lawrey on 09/07/2015.
 */
@FunctionalInterface
public interface SimpleSubscriptionFactory<E> {
    SimpleSubscription<E> create(Reference<E> reference, Function<Object, E> valueReader);
}
