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
import org.jetbrains.annotations.Nullable;

/**
 * This interface defines a means of visiting a resource for retrieving derived values and replacing the state of the Asset.
 * <p></p>
 * This interface allows visiting an individual resource.
 * <p></p>
 * The default implementations are trivial, however further guarentees can be provided.
 * e.g. a remote client may have the code executed on the server atomically.
 */
public interface Visitable<E> {

    /**
     * @return the current state of a visitable Asset
     */
    E get();

    /**
     * @param e replace the current state of the visitable Asset
     */
    void set(E e);

    /**
     * Apply a function to the visitable and return the result.  This function is assumed to not change the value in any significant way.
     *
     * @param function to apply e.g. call a getter
     * @param <R>      data type to return.
     * @return the result of the code called.
     */
    default <R> R applyTo(@NotNull SerializableFunction<E, R> function) {
        return function.apply(get());
    }

    /**
     * Apply a function to visitable potentially asynchronously.  This function is assumed to replace the value and trigger and events or replciated changes.
     *
     * @param updateFunction to update the state of the visitiable.
     */
    default void asyncUpdate(@NotNull SerializableFunction<E, E> updateFunction) {
        set(updateFunction.apply(get()));
    }

    /**
     * Apply a function to update the state of a visible, and apply a function to return a result object synchronously.
     *
     * @param updateFunction update to apply to the value.
     * @param returnFunction derive an object to return
     * @param <R> data type to return.
     * @return the result of the code called.
     */
    default <R> R syncUpdate(@NotNull SerializableFunction<E, E> updateFunction, @NotNull SerializableFunction<E, R> returnFunction) {
        E e = updateFunction.apply(get());
        set(e);
        return returnFunction.apply(e);
    }

    /**
     * Apply a function which takes an argument.  This argument may contain a combination of data. This function is assumed to not change the value in any significant way.
     *
     * @param function to apply
     * @param argument for the functions use.
     * @param <T> type of the argument
     * @param <R> type of the return value.
     * @return data derived.
     */
    default <T, R> R applyTo(@NotNull SerializableBiFunction<E, T, R> function, T argument) {
        return function.apply(get(), argument);
    }

    /**
     * Apply a function to visitable potentially asynchronously.  This argument may contain a combination of data. This function is assumed to replace the value and trigger and events or replciated changes.
     *
     * @param updateFunction to update the state of the visitiable.
     * @param argument for the functions use.
     * @param <T> type of the argument
     */
    default <T> void asyncUpdate(@NotNull SerializableBiFunction<E, T, E> updateFunction, T argument) {
        set(updateFunction.apply(get(), argument));
    }

    /**
     * Apply a function to update the state of a visible, and apply a function to return a result object synchronously. This argument may contain a combination of data. Optionally the arguments could be null.
     *
     * @param updateFunction update to apply to the value.
     * @param updateArgument for the update function to use.
     * @param returnFunction derive an object to return
     * @param returnArgument for the return value function to use
     * @param <R>            data type to return.
     * @return the result of the code called.
     */
    default <UT, RT, R> R syncUpdate(@NotNull SerializableBiFunction<E, UT, E> updateFunction, @Nullable UT updateArgument,
                                     @NotNull SerializableBiFunction<E, RT, R> returnFunction, @Nullable RT returnArgument) {
        E e = updateFunction.apply(get(), updateArgument);
        set(e);
        return returnFunction.apply(e, returnArgument);
    }
}
