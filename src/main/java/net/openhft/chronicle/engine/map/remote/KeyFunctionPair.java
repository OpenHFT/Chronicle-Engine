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

import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

/**
 * Created by peter on 07/07/15.
 */
public class KeyFunctionPair implements Marshallable {
    @NotNull
    Object key;

    @NotNull
    Object function;

    KeyFunctionPair(@NotNull Object key, @NotNull Object function) {
        this.key = key;
        assert Serializable.class.isAssignableFrom(function.getClass());
        this.function = function;
    }

    @NotNull
    public static KeyFunctionPair of(Object key, @NotNull Object function) {
        return new KeyFunctionPair(key, function);
    }

    @SuppressWarnings("ConstantConditions")
    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        wire.read(() -> "key").object(Object.class, this, (o, x) -> o.key = x)
                .read(() -> "function").object(Object.class, this, (o, f) -> o.function = f);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(() -> "key").object(key)
                .write(() -> "function").object(function);
    }
}
