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

package net.openhft.chronicle.engine.collection;

import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.io.StreamCorruptedException;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static net.openhft.chronicle.engine.collection.CollectionWireHandler.Params.key;
import static net.openhft.chronicle.engine.collection.CollectionWireHandler.Params.segment;

/**
 * @param <U> the type of each element in that collection
 */
public interface CollectionWireHandler<U, C extends Collection<U>> {

    void process(Wire in,
                 Wire out,
                 C collection,
                 CharSequence csp,
                 BiConsumer<ValueOut, U> toWire,
                 Function<ValueIn, U> fromWire,
                 Supplier<C> factory,
                 long tid) throws StreamCorruptedException;

    enum Params implements WireKey {
        key,
        segment,
    }

    enum SetEventId implements ParameterizeWireKey {
        size,
        isEmpty,
        add,
        addAll,
        retainAll,
        containsAll,
        removeAll,
        clear,
        remove(key),
        numberOfSegments,
        contains(key),
        identifier,
        iterator(segment);

        private final WireKey[] params;

        <P extends WireKey> SetEventId(P... params) {
            this.params = params;
        }

        @NotNull
        public <P extends WireKey> P[] params() {
            return (P[]) this.params;
        }
    }
}
