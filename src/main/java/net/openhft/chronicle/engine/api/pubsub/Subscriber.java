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
