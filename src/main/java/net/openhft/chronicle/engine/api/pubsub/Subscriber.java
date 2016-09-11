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

import net.openhft.chronicle.core.Jvm;

import java.util.function.Consumer;

/**
 * Subscriber to events of a specific topic/key.
 */
@FunctionalInterface
public interface Subscriber<E> extends ISubscriber, Consumer<E> {

    /**
     * Called when there is an event.
     *
     * @param e event
     * @throws InvalidSubscriberException to throw when this subscriber is no longer valid.
     */
    void onMessage(E e) throws InvalidSubscriberException;

    @Override
    default void accept(E e) {
        try {
            onMessage(e);

        } catch (InvalidSubscriberException ise) {
            throw Jvm.rethrow(ise);
        }
    }
}
