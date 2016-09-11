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

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.query.Filter;
import org.jetbrains.annotations.NotNull;

/**
 * Internal API for managing and monitoring subscriptions.
 */
public interface SubscriptionCollection<E> extends Closeable {
    void registerSubscriber(@NotNull RequestContext rc,
                            @NotNull Subscriber<E> subscriber,
                            @NotNull Filter<E> filter);

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
