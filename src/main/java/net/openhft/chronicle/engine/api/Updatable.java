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
import net.openhft.chronicle.core.util.SerializableUpdater;
import net.openhft.chronicle.core.util.SerializableUpdaterWithArg;
import org.jetbrains.annotations.NotNull;

/**
 * This interface assumes that the resource visited is mutable and will be update rather than replaced.
 */
public interface Updatable<E> {
    /**
     * Apply a function to an Updatable which is assumed to not change the visited in any significant way.  This might not trigger an event.
     *
     * @param function to apply to derive data from this Updatable.
     * @param <R>      return type.
     * @return derived data.
     */
    default <R> R applyTo(@NotNull SerializableFunction<E, R> function) {
        return function.apply((E) this);
    }

    /**
     * Update an Updatable potentially asynchronously.  This function is assumed to update Updateable.
     *
     * @param updateFunction to apply.
     */
    default void asyncUpdate(@NotNull SerializableUpdater<E> updateFunction) {
        updateFunction.accept((E) this);
    }

    /**
     * Apply a function to update an Updatable and apply a seconf function to derive some information from it.
     *
     * @param updateFunction to apply the update.
     * @param returnFunction to derive some data
     * @return value derived.
     */
    default <R> R syncUpdate(@NotNull SerializableUpdater<E> updateFunction, @NotNull SerializableFunction<E, R> returnFunction) {
        updateFunction.accept((E) this);
        return returnFunction.apply((E) this);
    }

    /**
     * Apply a function to an Updatable which is assumed to not change the visited in any significant way.  This might not trigger an event.
     *
     * @param function to apply to derive data from this Updatable.
     * @param <R>      return type.
     * @return derived data.
     */
    default <A, R> R applyTo(@NotNull SerializableBiFunction<E, A, R> function, A arg) {
        return function.apply((E) this, arg);
    }

    /**
     * Update an Updatable potentially asynchronously.  This function is assumed to update Updateable.
     *
     * @param updateFunction to apply.
     */
    default <A> void asyncUpdate(@NotNull SerializableUpdaterWithArg<E, A> updateFunction, A arg) {
        updateFunction.accept((E) this, arg);
    }

    /**
     * Apply a function to update an Updatable and apply a seconf function to derive some information from it.
     *
     * @param updateFunction to apply the update.
     * @param returnFunction to derive some data
     * @return value derived.
     */
    default <UA, RA, R> R syncUpdate(@NotNull SerializableUpdaterWithArg<E, UA> updateFunction, UA ua, @NotNull SerializableBiFunction<E, RA, R> returnFunction, RA ra) {
        updateFunction.accept((E) this, ua);
        return returnFunction.apply((E) this, ra);
    }
}
