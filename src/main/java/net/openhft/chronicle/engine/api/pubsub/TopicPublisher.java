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
import net.openhft.chronicle.engine.api.tree.KeyedView;
import org.jetbrains.annotations.NotNull;

/**
 * Publish to any topic in an Asset group.
 */
public interface TopicPublisher<T, M> extends KeyedView {
    /**
     * Publish to a provided topic.
     *
     * @param topic   to publish to
     * @param message to publish.
     */
    void publish(@NotNull T topic, @NotNull M message);

    /**
     * Add a subscription to this group.
     *
     * @param topicSubscriber to listen to events.
     * @throws AssetNotFoundException if the Asset is no longer valid.
     */
    void registerTopicSubscriber(@NotNull TopicSubscriber<T, M> topicSubscriber) throws AssetNotFoundException;

    /**
     * Unregister a TopicSubscriber
     *
     * @param topicSubscriber to unregister
     */
    void unregisterTopicSubscriber(@NotNull TopicSubscriber<T, M> topicSubscriber);

    Publisher<M> publisher(@NotNull T topic);

    void registerSubscriber(@NotNull T topic, @NotNull Subscriber<M> subscriber);
}
