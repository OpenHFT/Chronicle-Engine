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

package net.openhft.chronicle.engine.pubsub;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Reference;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.query.Filter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Function;

/**
 * Created by peter on 29/05/15.
 */
public class VanillaSimpleSubscription<E> implements SimpleSubscription<E> {

    private static final Logger LOG = LoggerFactory.getLogger(VanillaSimpleSubscription.class);

    private final Set<Subscriber<E>> subscribers = new CopyOnWriteArraySet<>();
    private final Reference<E> currentValue;
    private final Function<Object, E> valueReader;

    public VanillaSimpleSubscription(Reference<E> reference, Function<Object, E> valueReader) {
        this.currentValue = reference;
        this.valueReader = valueReader;
    }

    @Override
    public void registerSubscriber(@NotNull RequestContext rc, @NotNull Subscriber<E> subscriber, @NotNull Filter<E> filter) {
        subscribers.add(subscriber);
        if (rc.bootstrap() != Boolean.FALSE)
            try {
                subscriber.onMessage(currentValue.get());
            } catch (InvalidSubscriberException e) {
                subscribers.remove(subscriber);
            }
    }

    @Override
    public void unregisterSubscriber(@NotNull Subscriber subscriber) {
        subscribers.remove(subscriber);
        subscriber.onEndOfSubscription();
    }

    @Override
    public int keySubscriberCount() {
        return subscriberCount();
    }

    @Override
    public int entrySubscriberCount() {
        return 0;
    }

    @Override
    public int topicSubscriberCount() {
        return 0;
    }

    @Override
    public int subscriberCount() {
        return subscribers.size();
    }

    @Override
    public void notifyMessage(Object e) {
        try {
            E ee = e instanceof BytesStore ? valueReader.apply(e) : (E) e;
            SubscriptionConsumer.notifyEachSubscriber(subscribers, s -> s.onMessage(ee));
        } catch (ClassCastException e1) {
            System.err.println("Is " + valueReader + " the correct ValueReader?");
            throw e1;
        }
    }

    @Override
    public void close() {
        for (Subscriber<E> subscriber : subscribers) {
            try {
                subscriber.onEndOfSubscription();
            } catch (Exception e) {
                LOG.error("", e);
            }
        }
    }
}
