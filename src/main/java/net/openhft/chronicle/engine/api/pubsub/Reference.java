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

import net.openhft.chronicle.engine.api.Visitable;

import java.util.function.Supplier;

public interface Reference<E> extends Publisher<E>, Supplier<E>, Visitable<E> {
    E get();

    default E getAndSet(E e) {
        E prev = get();
        set(e);
        return prev;
    }

    void set(E e);

    void remove();

    default E getAndRemove() {
        E prev = get();
        remove();
        return prev;
    }

    default void publish(E e) {
        set(e);
    }

}
