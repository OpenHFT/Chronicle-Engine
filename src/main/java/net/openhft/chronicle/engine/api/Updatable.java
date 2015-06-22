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

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by peter on 22/06/15.
 */
public interface Updatable<E> {
    default <R> R apply(Function<E, R> function) {
        return function.apply((E) this);
    }

    default void asyncUpdate(Consumer<E> updateFunction) {
        updateFunction.accept((E) this);
    }

    default <R> R syncUpdate(Consumer<E> updateFunction, Function<E, R> returnFunction) {
        updateFunction.accept((E) this);
        return returnFunction.apply((E) this);
    }
}
