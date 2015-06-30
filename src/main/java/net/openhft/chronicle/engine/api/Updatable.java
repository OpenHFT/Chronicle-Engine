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
 * Created by peter on 22/06/15.
 */
public interface Updatable<E> {
    default <R> R apply(@NotNull SerializableFunction<E, R> function) {
        return function.apply((E) this);
    }

    default void asyncUpdate(@NotNull SerializableUpdater<E> updateFunction) {
        updateFunction.accept((E) this);
    }


    default <R> R syncUpdate(@NotNull SerializableUpdater<E> updateFunction, @NotNull SerializableFunction<E, R> returnFunction) {
        updateFunction.accept((E) this);
        return returnFunction.apply((E) this);
    }

    default <A, R> R apply(@NotNull SerializableBiFunction<E, A, R> function, A arg) {
        return function.apply((E) this, arg);
    }

    default <A> void asyncUpdate(@NotNull SerializableUpdaterWithArg<E, A> updateFunction, A arg) {
        updateFunction.accept((E) this, arg);
    }

    default <UA, RA, R> R syncUpdate(@NotNull SerializableUpdaterWithArg<E, UA> updateFunction, UA ua, @NotNull SerializableBiFunction<E, RA, R> returnFunction, RA ra) {
        updateFunction.accept((E) this, ua);
        return returnFunction.apply((E) this, ra);
    }
}
