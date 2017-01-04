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
            @NotNull KeyValuePair kf = (KeyValuePair) o;
            return map.remove(convertTo(kClass, kf.key), convertTo(vClass, kf.value));
        }
    },
    REPLACE {
        @Override
        public Object apply(@NotNull MapView map, Object o) {
            Class kClass = map.keyType();
            Class vClass = map.valueType();
            if (o instanceof KeyValuePair) {
                @NotNull KeyValuePair kf = (KeyValuePair) o;
                return map.replace(convertTo(kClass, kf.key), convertTo(vClass, kf.value));
            }
            @NotNull KeyValuesTuple kf = (KeyValuesTuple) o;
            return map.replace(convertTo(kClass, kf.key), convertTo(vClass, kf.oldValue), convertTo(vClass, kf.value));
        }
    },
    PUT_IF_ABSENT {
        @Override
        public Object apply(@NotNull MapView map, Object o) {
            Class kClass = map.keyType();
            Class vClass = map.valueType();
            @NotNull KeyValuePair kf = (KeyValuePair) o;
            return map.putIfAbsent(convertTo(kClass, kf.key), convertTo(vClass, kf.value));
        }
    },
    COMPUTE_IF_ABSENT {
        @Override
        public Object apply(@NotNull MapView map, Object o) {
            Class kClass = map.keyType();
            @NotNull KeyFunctionPair kf = (KeyFunctionPair) o;
            return map.computeIfAbsent(convertTo(kClass, kf.key), (Function) kf.function);
        }
    },
    COMPUTE_IF_PRESENT {
        @Override
        public Object apply(@NotNull MapView map, Object o) {
            Class kClass = map.keyType();
            @NotNull KeyFunctionPair kf = (KeyFunctionPair) o;
            return map.computeIfPresent(convertTo(kClass, kf.key), (BiFunction) kf.function);
        }
    },
    COMPUTE {
        @Override
        public Object apply(@NotNull MapView map, Object o) {
            Class kClass = map.keyType();
            @NotNull KeyFunctionPair kf = (KeyFunctionPair) o;
            return map.compute(convertTo(kClass, kf.key), (BiFunction) kf.function);
        }
    },
    MERGE {
        @Override
        public Object apply(@NotNull MapView map, Object o) {
            Class kClass = map.keyType();
            Class vClass = map.valueType();
            @NotNull KeyValueFunctionTuple kvf = (KeyValueFunctionTuple) o;
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
