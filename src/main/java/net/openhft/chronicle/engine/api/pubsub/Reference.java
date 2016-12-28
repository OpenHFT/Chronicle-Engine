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

import net.openhft.chronicle.engine.api.Visitable;
import org.jetbrains.annotations.Nullable;

import java.util.function.Supplier;

/**
 * Hold a reference to a specific topic/key.
 */
public interface Reference<E> extends Publisher<E>, Supplier<E>, Visitable<E> {
    /**
     * @return the current value.
     */
    @Nullable
    E get();

    /**
     * Set a new value and return the old value.
     *
     * @param e to set
     * @return the old value.
     */
    @Nullable
    default E getAndSet(E e) {
        @Nullable E prev = get();
        set(e);
        return prev;
    }

    /**
     * Set the new value
     *
     * @param e replace value
     */
    long set(E e);

    /**
     * Remove the topic/key
     */
    void remove();

    /**
     * Remove the topic/key and return the old value
     *
     * @return the old value.
     */
    @Nullable
    default E getAndRemove() {
        @Nullable E prev = get();
        remove();
        return prev;
    }

    /**
     * Publish to this topic/key.
     *
     * @param e value to publish/set
     */
    default void publish(E e) {
        set(e);
    }

    default Class getType() {
        throw new UnsupportedOperationException();
    }
}
