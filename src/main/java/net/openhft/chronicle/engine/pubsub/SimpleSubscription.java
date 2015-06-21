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
            E ee = e instanceof BytesStore ? valueReader.readFrom(e, null) : (E) e;
            SubscriptionConsumer.notifyEachSubscriber(subscribers, s -> s.onMessage(ee));
        } catch (ClassCastException e1) {
            System.err.println("Is " + valueReader + " the correct ValueReader?");
            throw e1;
        }
    }

    @Override
    public boolean keyedView() {
        return false;
    }

    @Override
    public void close() {
        for (Subscriber<E> subscriber : subscribers) {
            try {
                subscriber.onEndOfSubscription();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
