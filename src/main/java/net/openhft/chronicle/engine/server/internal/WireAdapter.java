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

package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;
import org.jetbrains.annotations.NotNull;

import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;

public interface WireAdapter<K, V> {

    @NotNull
    BiConsumer<ValueOut, K> keyToWire();

    @NotNull
    Function<ValueIn, K> wireToKey();

    @NotNull
    BiConsumer<ValueOut, V> valueToWire();

    @NotNull
    Function<ValueIn, V> wireToValue();

    @NotNull
    BiConsumer<ValueOut, Entry<K, V>> entryToWire();

    @NotNull
    Function<ValueIn, Entry<K, V>> wireToEntry();
}
