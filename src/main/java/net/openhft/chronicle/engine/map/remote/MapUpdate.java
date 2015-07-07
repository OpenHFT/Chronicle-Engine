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

import net.openhft.chronicle.core.util.SerializableUpdaterWithArg;
import net.openhft.chronicle.engine.api.map.MapView;

import java.util.Map;
import java.util.function.BiFunction;

/**
 * Created by peter on 07/07/15.
 */
public enum MapUpdate implements SerializableUpdaterWithArg<MapView, Object> {
    PUT_ALL {
        @Override
        public void accept(MapView map, Object mapToPut) {
            map.putAll((Map) mapToPut);
        }
    },
    REPLACE_ALL {
        @Override
        public void accept(MapView map, Object o) {
            BiFunction function = (BiFunction) o;
            map.replaceAll(function);
        }
    };
}
