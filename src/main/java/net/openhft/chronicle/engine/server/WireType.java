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

package net.openhft.chronicle.engine.server;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.Wire;

import java.util.function.Function;

/**
 * Created by Rob Austin
 */
@Deprecated
public enum WireType implements Function<Bytes, Wire> {
    TEXT {
        @Override
        public Wire apply(Bytes bytes) {
            return new TextWire(bytes);
        }
    }, BINARY {
        @Override
        public Wire apply(Bytes bytes) {
            return new BinaryWire(bytes);
        }
    };

    // todo to be removed
    @Deprecated
    public static Function<Bytes, Wire> wire = BINARY;
}
