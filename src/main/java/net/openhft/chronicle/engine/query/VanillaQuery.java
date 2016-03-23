/*
 *
 *  *     Copyright (C) ${YEAR}  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.engine.query;

import net.openhft.chronicle.core.util.SerializableFunction;
import net.openhft.chronicle.core.util.SerializablePredicate;
import net.openhft.chronicle.engine.api.query.Query;
import net.openhft.chronicle.engine.api.query.Subscription;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * Created by peter.lawrey on 12/07/2015.
 */
public class VanillaQuery<E> implements Query<E> {
    private final Stream<E> stream;

    public VanillaQuery(Stream<E> stream) {
        this.stream = stream;
    }

    @NotNull
    @Override
    public Query<E> filter(SerializablePredicate<? super E> predicate) {
        return new VanillaQuery<>(stream.filter(predicate));
    }

    @NotNull
    @Override
    public <R> Query<R> map(SerializableFunction<? super E, ? extends R> mapper) {
        return new VanillaQuery<>(stream.map(mapper));
    }

    @NotNull
    @Override
    public <R> Query<R> project(Class<R> rClass) {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    @Override
    public <R> Query<R> flatMap(@NotNull SerializableFunction<? super E, ? extends Query<? extends R>> mapper) {
        return new VanillaQuery<>(stream.flatMap(e -> mapper.apply(e).stream()));
    }

    @Override
    public Stream<E> stream() {
        return stream;
    }

    @Override
    public Subscription subscribe(Consumer<? super E> action) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <R, A> R collect(Collector<? super E, A, R> collector) {
        return stream.collect(collector);
    }

    @Override
    public void forEach(Consumer<? super E> action) {
        stream.forEach(action);
    }
}
