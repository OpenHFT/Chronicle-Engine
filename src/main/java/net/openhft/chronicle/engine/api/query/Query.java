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

package net.openhft.chronicle.engine.api.query;

import net.openhft.chronicle.core.util.SerializableFunction;
import net.openhft.chronicle.core.util.SerializablePredicate;

import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * Created by peter.lawrey on 11/07/2015.
 */
public interface Query<T> {

    Query<T> filter(SerializablePredicate<? super T> predicate);

    <R> Query<R> map(SerializableFunction<? super T, ? extends R> mapper);

    <R> Query<R> project(Class<R> rClass);

    <R> Query<R> flatMap(SerializableFunction<? super T, ? extends Query<? extends R>> mapper);

    Stream<T> stream();

    Subscription subscribe(Consumer<? super T> action);

    <R, A> R collect(Collector<? super T, A, R> collector);

    void forEach(Consumer<? super T> action);
}
