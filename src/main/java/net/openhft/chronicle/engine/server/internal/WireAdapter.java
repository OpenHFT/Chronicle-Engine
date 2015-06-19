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

package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.ValueOut;

import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;

public interface WireAdapter<K, V> {

    BiConsumer<ValueOut, K> keyToWire();

    Function<ValueIn, K> wireToKey();

    BiConsumer<ValueOut, V> valueToWire();

    Function<ValueIn, V> wireToValue();

    BiConsumer<ValueOut, Entry<K, V>> entryToWire();

    Function<ValueIn, Entry<K, V>> wireToEntry();

}
