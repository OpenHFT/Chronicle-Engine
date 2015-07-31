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

import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;

/**
 * A handle to Publish to a specific topic.
 */
public interface Publisher<E> {
    /**
     * Publish an event
     *
     * @param event to publish
     */
    void publish(E event);

    /**
     * Add a subscription to this specific topic
     *
     * @param bootstrap  to bootstrap
     * @param subscriber to register
     * @throws AssetNotFoundException if the topic no longer exists.
     */
    void registerSubscriber(boolean bootstrap, Subscriber<E> subscriber) throws AssetNotFoundException;

    /**
     * Remove a subscriber
     *
     * @param subscriber to remove
     */
    void unregisterSubscriber(Subscriber<E> subscriber);

    /**
     * @return the number of subscriptions.
     */
    int subscriberCount();
}
