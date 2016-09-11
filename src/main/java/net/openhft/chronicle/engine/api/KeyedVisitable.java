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
 * The default implementations are trivial, however further guarantees can be provided.
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
     * @param key      to visit within this collection.
     * @param function to apply e.g. call a getter
     * @param <R>      data type to return.
     * @return the result of the code called.
     */
    default <R> R applyToKey(K key, @NotNull SerializableFunction<E, R> function) {
        return function.apply(get(key));
    }

    /**
     * Apply a function to visitable potentially asynchronously.  This function is assumed to replace
     * the value and trigger and events or replicated changes.
     *
     * @param key            to visit within this collection.
     * @param updateFunction to update the state of the visibility.
     */
    default void asyncUpdateKey(K key, @NotNull SerializableFunction<E, E> updateFunction) {
        set(key, updateFunction.apply(get(key)));
    }

    /**
     * Apply a function to update the state of a visible, and apply a function to return a result object synchronously.
     *
     * @param key            to update within this collection.
     * @param updateFunction update to apply to the value.
     * @param returnFunction derive an object to return
     * @param <R>            data type to return.
     * @return the result of the code called.
     */
    default <R> R syncUpdateKey(K key, @NotNull SerializableFunction<E, E> updateFunction, @NotNull SerializableFunction<E, R> returnFunction) {
        E e = updateFunction.apply(get(key));
        set(key, e);
        return returnFunction.apply(e);
    }

    /**
     * Apply a function which takes an argument.  This argument may contain a combination of data. This function is assumed to not change the value in any significant way.
     *
     * @param key      to visit within this collection.
     * @param function to apply
     * @param argument for the functions use.
     * @param <T>      type of the argument
     * @param <R>      type of the return value.
     * @return data derived.
     */
    default <T, R> R applyToKey(K key, @NotNull SerializableBiFunction<E, T, R> function, T argument) {
        return function.apply(get(key), argument);
    }

    /**
     * Apply a function to visitable potentially asynchronously.  This argument may contain a combination of data. This function is assumed to replace the value and trigger and events or replciated changes.
     *
     * @param key            to update within this collection
     * @param updateFunction to update the state of the visitable.
     * @param argument       for the functions use.
     * @param <T>            type of the argument
     */
    default <T> void asyncUpdateKey(K key, @NotNull SerializableBiFunction<E, T, E> updateFunction, T argument) {
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
    default <T, RT, R> R syncUpdateKey(K key, @NotNull SerializableBiFunction<E, T, E> updateFunction, @Nullable T updateArgument,
                                       @NotNull SerializableBiFunction<E, RT, R> returnFunction, @Nullable RT returnArgument) {
        E e = updateFunction.apply(get(key), updateArgument);
        set(key, e);
        return returnFunction.apply(e, returnArgument);
    }
}
