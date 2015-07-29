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

package net.openhft.chronicle.engine.map.remote;

import net.openhft.chronicle.core.util.SerializableBiFunction;
import net.openhft.chronicle.engine.api.map.MapView;
import org.jetbrains.annotations.NotNull;

import java.util.function.BiFunction;
import java.util.function.Function;

import static net.openhft.chronicle.core.util.ObjectUtils.convertTo;

/**
 * Created by peter on 07/07/15.
 */
public enum MapFunction implements SerializableBiFunction<MapView, Object, Object> {
    CONTAINS_VALUE {
        @Override
        public Boolean apply(@NotNull MapView map, Object value) {
            Class vClass = map.valueType();
            return map.containsValue(convertTo(vClass, value));
        }
    },
    REMOVE {
        @Override
        public Object apply(@NotNull MapView map, Object o) {
            Class kClass = map.keyType();
            Class vClass = map.valueType();
            KeyValuePair kf = (KeyValuePair) o;
            return map.remove(convertTo(kClass, kf.key), convertTo(vClass, kf.value));
        }
    },
    REPLACE {
        @Override
        public Object apply(@NotNull MapView map, Object o) {
            Class kClass = map.keyType();
            Class vClass = map.valueType();
            if (o instanceof KeyValuePair) {
                KeyValuePair kf = (KeyValuePair) o;
                return map.replace(convertTo(kClass, kf.key), convertTo(vClass, kf.value));
            }
            KeyValuesTuple kf = (KeyValuesTuple) o;
            return map.replace(convertTo(kClass, kf.key), convertTo(vClass, kf.oldValue), convertTo(vClass, kf.value));
        }
    },
    PUT_IF_ABSENT {
        @Override
        public Object apply(@NotNull MapView map, Object o) {
            Class kClass = map.keyType();
            Class vClass = map.valueType();
            KeyValuePair kf = (KeyValuePair) o;
            return map.putIfAbsent(convertTo(kClass, kf.key), convertTo(vClass, kf.value));
        }
    },
    COMPUTE_IF_ABSENT {
        @Override
        public Object apply(@NotNull MapView map, Object o) {
            Class kClass = map.keyType();
            KeyFunctionPair kf = (KeyFunctionPair) o;
            return map.computeIfAbsent(convertTo(kClass, kf.key), (Function) kf.function);
        }
    },
    COMPUTE_IF_PRESENT {
        @Override
        public Object apply(@NotNull MapView map, Object o) {
            Class kClass = map.keyType();
            KeyFunctionPair kf = (KeyFunctionPair) o;
            return map.computeIfPresent(convertTo(kClass, kf.key), (BiFunction) kf.function);
        }
    },
    COMPUTE {
        @Override
        public Object apply(@NotNull MapView map, Object o) {
            Class kClass = map.keyType();
            KeyFunctionPair kf = (KeyFunctionPair) o;
            return map.compute(convertTo(kClass, kf.key), (BiFunction) kf.function);
        }
    },
    MERGE {
        @Override
        public Object apply(@NotNull MapView map, Object o) {
            Class kClass = map.keyType();
            Class vClass = map.valueType();
            KeyValueFunctionTuple kvf = (KeyValueFunctionTuple) o;
            return map.merge(convertTo(kClass, kvf.key), convertTo(vClass, kvf.value), (BiFunction) kvf.function);
        }
    },
    HASH_CODE {
        @Override
        public Object apply(@NotNull MapView map, Object ignored) {
            return map.hashCode();
        }
    },
    EQUALS {
        @Override
        public Object apply(@NotNull MapView map, Object o) {
            return map.equals(o);
        }
    }
}
