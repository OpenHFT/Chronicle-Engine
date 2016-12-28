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

import net.openhft.chronicle.core.util.SerializableUpdaterWithArg;
import net.openhft.chronicle.engine.api.map.MapView;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.function.BiFunction;

/**
 * Created by peter on 07/07/15.
 */
public enum MapUpdate implements SerializableUpdaterWithArg<MapView, Object> {
    PUT_ALL {
        @Override
        public void accept(@NotNull MapView map, Object mapToPut) {
            map.putAll((Map) mapToPut);
        }
    },
    REPLACE_ALL {
        @Override
        public void accept(@NotNull MapView map, Object o) {
            @NotNull BiFunction function = (BiFunction) o;
            map.replaceAll(function);
        }
    }
}
