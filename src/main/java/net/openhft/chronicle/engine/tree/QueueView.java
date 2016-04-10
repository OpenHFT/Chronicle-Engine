/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.engine.api.pubsub.TopicPublisher;
import net.openhft.chronicle.engine.api.tree.KeyedView;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * @author Rob Austin.
 */
public interface QueueView<T, M> extends TopicPublisher<T, M>, KeyedView {

    /**
     * returns a {@link Excerpt} at a given index
     *
     * @param index the location of the except
     */
    @Nullable
    Excerpt<T, M> get(long index);

    /**
     * the next message from the current tailer which has this {@code topic}
     *
     * @param topic next excerpt that has this topic
     * @return the except
     */
    Excerpt<T, M> get(T topic);

    /**
     * Publish to a provided topic.
     *
     * @param topic   to publish to
     * @param message to publish.
     * @return the index in the chroncile queue the ex
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
