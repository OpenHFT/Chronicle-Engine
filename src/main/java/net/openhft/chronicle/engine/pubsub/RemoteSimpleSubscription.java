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

import net.openhft.chronicle.engine.api.pubsub.Reference;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

/**
 * Created by peter on 29/05/15.
 */
public class RemoteSimpleSubscription<E> implements SimpleSubscription<E> {
    // TODO CE-101 pass to the server
    private final Reference<E> reference;
    private final Function<Object, E> valueReader;

    public RemoteSimpleSubscription(Reference<E> reference, Function<Object, E> valueReader) {
        this.reference = reference;
        this.valueReader = valueReader;
    }

    @Override
    public void registerSubscriber(@NotNull RequestContext rc, @NotNull Subscriber<E> subscriber) {
        reference.registerSubscriber(rc.bootstrap() != Boolean.FALSE, subscriber);
    }

    @Override
    public void unregisterSubscriber(Subscriber<E> subscriber) {
        reference.unregisterSubscriber(subscriber);
    }

    @Override
    public int keySubscriberCount() {

        // TODO CE-101 pass to the server
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
        return reference.subscriberCount();
    }

    @Override
    public void notifyMessage(Object e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean keyedView() {
        return false;
    }

    @Override
    public void close() {
    }
}
