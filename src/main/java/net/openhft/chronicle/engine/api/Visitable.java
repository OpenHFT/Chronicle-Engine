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

package net.openhft.chronicle.engine.api;

import net.openhft.chronicle.core.util.SerializableBiFunction;
import net.openhft.chronicle.core.util.SerializableFunction;
import org.jetbrains.annotations.NotNull;

/**
 * Created by peter on 22/06/15.
 */
public interface Visitable<E> {
    E get();

    void set(E e);

    default <R> R apply(@NotNull SerializableFunction<E, R> function) {
        return function.apply(get());
    }

    default void asyncUpdate(@NotNull SerializableFunction<E, E> updateFunction) {
        set(updateFunction.apply(get()));
    }

    default <R> R syncUpdate(@NotNull SerializableFunction<E, E> updateFunction, @NotNull SerializableFunction<E, R> returnFunction) {
        E e = updateFunction.apply(get());
        set(e);
        return returnFunction.apply(e);
    }

    default <T, R> R apply(@NotNull SerializableBiFunction<E, T, R> function, T argument) {
        return function.apply(get(), argument);
    }

    default <T> void asyncUpdate(@NotNull SerializableBiFunction<E, T, E> updateFunction, T argument) {
        set(updateFunction.apply(get(), argument));
    }

    default <T, RT, R> R syncUpdate(@NotNull SerializableBiFunction<E, T, E> updateFunction, T updateArgument, @NotNull SerializableBiFunction<E, RT, R> returnFunction, RT returnArgument) {
        E e = updateFunction.apply(get(), updateArgument);
        set(e);
        return returnFunction.apply(e, returnArgument);
    }
}
