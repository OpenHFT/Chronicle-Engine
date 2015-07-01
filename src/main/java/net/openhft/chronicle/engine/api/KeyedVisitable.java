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
 * This interface allows visiting any element of a keyed collection of resources
 * <p></p>
 * The default implementations are trivial, however further guarentees can be provided.
 * e.g. a remote client may have the code executed on the server atomically.
 */
public interface KeyedVisitable<K, E> {

    /**
     * @param key to get within this collection.
     * @return the current state of a visitable Asset
     */
    @Nullable
    E get(K key);

    /**
     * @param key     to set within this collection.
     * @param element replace the current state of the visitable Asset
     */
    void set(K key, E element);

    /**
     * Apply a function to the visitable and return the result.  This function is assumed to not change the value in any significant way.
     *
     * @param key to visit within this collection.
     * @param function to apply e.g. call a getter
     * @param <R> data type to return.
     * @return the result of the code called.
     */
    default <R> R apply(K key, @NotNull SerializableFunction<E, R> function) {
        return function.apply(get(key));
    }

    /**
     * Apply a function to visitable potentially asynchronously.  This function is assumed to replace the value and trigger and events or replciated changes.
     *
     * @param key to visit within this collection.
     * @param updateFunction to update the state of the visitiable.
     */
    default void asyncUpdate(K key, @NotNull SerializableFunction<E, E> updateFunction) {
        set(key, updateFunction.apply(get(key)));
    }

    /**
     * Apply a function to update the state of a visible, and apply a function to return a result object synchronously.
     *
     * @param key to update within this collection.
     * @param updateFunction update to apply to the value.
     * @param returnFunction derive an object to return
     * @param <R> data type to return.
     * @return the result of the code called.
     */
    default <R> R syncUpdate(K key, @NotNull SerializableFunction<E, E> updateFunction, @NotNull SerializableFunction<E, R> returnFunction) {
        E e = updateFunction.apply(get(key));
        set(key, e);
        return returnFunction.apply(e);
    }

    /**
     * Apply a function which takes an argument.  This argument may contain a combination of data. This function is assumed to not change the value in any significant way.
     *
     * @param key to visit within this collection.
     * @param function to apply
     * @param argument for the functions use.
     * @param <T> type of the argument
     * @param <R> type of the return value.
     * @return data derived.
     */
    default <T, R> R apply(K key, @NotNull SerializableBiFunction<E, T, R> function, T argument) {
        return function.apply(get(key), argument);
    }

    /**
     * Apply a function to visitable potentially asynchronously.  This argument may contain a combination of data. This function is assumed to replace the value and trigger and events or replciated changes.
     *
     * @param key to update within this collection
     * @param updateFunction to update the state of the visitiable.
     * @param argument for the functions use.
     * @param <T> type of the argument
     */
    default <T> void asyncUpdate(K key, @NotNull SerializableBiFunction<E, T, E> updateFunction, T argument) {
        set(key, updateFunction.apply(get(key), argument));
    }

    /**
     * Apply a function to update the state of a visible, and apply a function to return a result object synchronously. This argument may contain a combination of data. Optionally the arguments could be null.
     *
     * @param key            to update within this collection
     * @param updateFunction update to apply to the value.
     * @param updateArgument for the update function to use.
     * @param returnFunction derive an object to return
     * @param returnArgument for the return value function to use
     * @param <R>            data type to return.
     * @return the result of the code called.
     */
    default <T, RT, R> R syncUpdate(K key, @NotNull SerializableBiFunction<E, T, E> updateFunction, @Nullable T updateArgument,
                                    @NotNull SerializableBiFunction<E, RT, R> returnFunction, @Nullable RT returnArgument) {
        E e = updateFunction.apply(get(key), updateArgument);
        set(key, e);
        return returnFunction.apply(e, returnArgument);
    }
}
