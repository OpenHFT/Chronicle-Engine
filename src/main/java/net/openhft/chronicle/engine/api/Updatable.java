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
    default <R> R apply(@NotNull SerializableFunction<E, R> function) {
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
     * @param <R> return type.
     * @return derived data.
     */
    default <A, R> R apply(@NotNull SerializableBiFunction<E, A, R> function, A arg) {
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
