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

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.api.tree.View;
import org.jetbrains.annotations.NotNull;

/**
 * Internal API for managing and monitoring subscriptions.
 */
public interface Subscription<E> extends View, Closeable {
    void registerSubscriber(@NotNull RequestContext rc, @NotNull Subscriber<E> subscriber);

    void unregisterSubscriber(@NotNull Subscriber subscriber);

    int keySubscriberCount();

    int entrySubscriberCount();

    int topicSubscriberCount();

    /**
     * @return total subscriber count.
     */
    default int subscriberCount() {
        return keySubscriberCount() + entrySubscriberCount() + topicSubscriberCount();
    }

}
