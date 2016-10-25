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

package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.engine.api.pubsub.TopicPublisher;
import net.openhft.chronicle.engine.api.tree.Assetted;
import net.openhft.chronicle.engine.api.tree.KeyedView;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * @author Rob Austin.
 */
public interface QueueView<T, M> extends TopicPublisher<T, M>, KeyedView, Assetted<Object> {

    /**
     * returns a {@link Excerpt} at a given index
     *
     * @param index the location of the except
     */
    @Nullable
    Excerpt<T, M> getExcerpt(long index);

    /**
     * the next message from the current tailer which has this {@code topic}
     *
     * @param topic next excerpt that has this topic
     * @return the except
     */
    Excerpt<T, M> getExcerpt(T topic);

    /**
     * Publish to a provided topic.
     *
     * @param topic   to publish to
     * @param message to publish.
     * @return the index in the chronicle queue the ex
     */
    long publishAndIndex(@NotNull T topic, @NotNull M message);


    interface Excerpt<T, M> {
        T topic();

        M message();

        long index();

        void clear();
    }

    interface Tailer<T, M> {
        /**
         * @return the next message from the current tailer
         */
        @Nullable
        Excerpt<T, M> read();
    }
}
