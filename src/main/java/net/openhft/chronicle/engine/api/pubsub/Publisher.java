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
     * @param bootstrap        to bootstrap
     * @param throttlePeriodMs {@code 0} for no throttling, otherwise the period of
     *                         throttling in milliseconds
     * @param subscriber       to register  @throws AssetNotFoundException if the topic no longer
     */
    void registerSubscriber(boolean bootstrap,
                            int throttlePeriodMs,
                            Subscriber<E> subscriber) throws AssetNotFoundException;

    /**
     * Remove a subscriber
     *
     * @param subscriber to remove
     */
    void unregisterSubscriber(Subscriber subscriber);

    /**
     * @return the number of subscriptions.
     */
    int subscriberCount();
}
