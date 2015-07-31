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

import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.Assetted;
import net.openhft.chronicle.engine.api.tree.View;

/**
 * Publish to any topic in an Asset group.
 */
public interface TopicPublisher<T, M> extends View, Assetted<MapView<T, M>> {
    /**
     * Publish to a provided topic.
     *
     * @param topic   to publish to
     * @param message to publish.
     */
    void publish(T topic, M message);

    /**
     * Add a subscription to this group.
     * @param topicSubscriber to listen to events.
     * @throws AssetNotFoundException if the Asset is no longer valid.
     */
    void registerTopicSubscriber(TopicSubscriber<T, M> topicSubscriber) throws AssetNotFoundException;

    /**
     * Unregister a TopicSubscriber
     *
     * @param topicSubscriber to unregister
     */
    void unregisterTopicSubscriber(TopicSubscriber<T, M> topicSubscriber);

    /**
     * @return this represents a keyed asset, i.e. anything under this must be a SubAsset.
     */
    default boolean keyedView() {
        return true;
    }
}
