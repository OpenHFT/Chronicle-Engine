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

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by peter on 07/07/15.
 */
public enum MapFunction implements SerializableBiFunction<MapView, Object, Object> {
    CONTAINS_VALUE {
        @Override
        public Boolean apply(MapView map, Object value) {
            return map.containsValue(value);
        }
    },
    REMOVE {
        @Override
        public Object apply(MapView mapView, Object o) {
            KeyValuePair kf = (KeyValuePair) o;
            return mapView.remove(kf.key, kf.value);
        }
    },
    REPLACE {
        @Override
        public Object apply(MapView mapView, Object o) {
            if (o instanceof KeyValuePair) {
                KeyValuePair kf = (KeyValuePair) o;
                return mapView.replace(kf.key, kf.value);
            }
            KeyValuesTuple kf = (KeyValuesTuple) o;
            return mapView.replace(kf.key, kf.oldValue, kf.value);
        }
    },
    PUT_IF_ABSENT {
        @Override
        public Object apply(MapView mapView, Object o) {
            KeyValuePair kf = (KeyValuePair) o;
            return mapView.putIfAbsent(kf.key, kf.value);
        }
    },
    COMPUTE_IF_ABSENT {
        @Override
        public Object apply(MapView mapView, Object o) {
            KeyFunctionPair kf = (KeyFunctionPair) o;
            return mapView.computeIfAbsent(kf.key, (Function) kf.function);
        }
    },
    COMPUTE_IF_PRESENT {
        @Override
        public Object apply(MapView mapView, Object o) {
            KeyFunctionPair kf = (KeyFunctionPair) o;
            return mapView.computeIfPresent(kf.key, (BiFunction) kf.function);
        }
    },
    COMPUTE {
        @Override
        public Object apply(MapView mapView, Object o) {
            KeyFunctionPair kf = (KeyFunctionPair) o;
            return mapView.compute(kf.key, (BiFunction) kf.function);
        }
    },
    MERGE {
        @Override
        public Object apply(MapView mapView, Object o) {
            KeyValueFunctionTuple kvf = (KeyValueFunctionTuple) o;
            return mapView.merge(kvf.key, kvf.value, (BiFunction) kvf.function);
        }
    },
    HASH_CODE {
        @Override
        public Object apply(MapView mapView, Object ignored) {
            return mapView.hashCode();
        }
    },
    EQUALS {
        @Override
        public Object apply(MapView mapView, Object o) {
            return mapView.equals(o);
        }
    };
}
