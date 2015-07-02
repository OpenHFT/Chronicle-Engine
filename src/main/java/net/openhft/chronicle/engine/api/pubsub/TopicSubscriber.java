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

/**
 * Subscriber for a pair of topic and message on an Asset group.
 */
@FunctionalInterface
public interface TopicSubscriber<T, M> extends ISubscriber {
    /**
     * Called when a topic in a group has an new message/event
     *
     * @param topic   the message was associated with
     * @param message published
     * @throws InvalidSubscriberException to throw when this subscriber is no longer valid.
     */
    void onMessage(T topic, M message) throws InvalidSubscriberException;
}
