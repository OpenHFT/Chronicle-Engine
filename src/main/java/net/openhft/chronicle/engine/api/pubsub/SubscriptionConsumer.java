/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
                        Jvm.warn().on(getClass(), e);
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
