/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;

import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;

public interface MapHandler<K, V> {

    BiConsumer<ValueOut, K> keyToWire();

    Function<ValueIn, K> wireToKey();

    BiConsumer<ValueOut, V> valueToWire();

    Function<ValueIn, V> wireToValue();

    BiConsumer<ValueOut, Entry<K, V>> entryToWire();

    Function<ValueIn, Entry<K, V>> wireToEntry();

}
