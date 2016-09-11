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
