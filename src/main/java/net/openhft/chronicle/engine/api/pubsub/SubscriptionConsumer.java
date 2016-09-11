/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.engine.api.pubsub;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.util.ThrowingConsumer;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Internal API for notifying events.
 */
@FunctionalInterface
public interface SubscriptionConsumer<T> {
    Logger LOG = LoggerFactory.getLogger(SubscriptionConsumer.class);

    static <S extends ISubscriber> void notifyEachSubscriber(@NotNull Set<S> subs, @NotNull SubscriptionConsumer<S> doNotify) {
        doNotify.notifyEachSubscriber(subs);
    }

    static <E> void notifyEachEvent(@NotNull Set<E> subs, @NotNull SubscriptionConsumer<E> doNotify) throws InvalidSubscriberException {
        doNotify.notifyEachEvent(subs);
    }

    default void notifyEachSubscriber(@NotNull Set<T> subs) {
        subs.forEach(s -> {
            try {
                accept(s);

            } catch (InvalidSubscriberException ise) {
                subs.remove(s);
                if (s instanceof ISubscriber) {
                    try {
                        ((ISubscriber) s).onEndOfSubscription();

                    } catch (RuntimeException e) {
                        Jvm.debug().on(getClass(), e);
                    }
                }
            }
        });
    }

    default void notifyEachEvent(@NotNull Set<T> subs) throws InvalidSubscriberException {
        subs.forEach(ThrowingConsumer.asConsumer(this::accept));
    }

    void accept(T subscriber) throws InvalidSubscriberException;
}
