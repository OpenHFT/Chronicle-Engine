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

package net.openhft.chronicle.engine.queue;

import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class MyMarshallable implements Marshallable {

    @Nullable
    String s;

    public MyMarshallable(String s) {
        this.s = s;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
        s = wire.read().text();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write().text(s);
    }

    @Nullable
    @Override
    public String toString() {
        return s;
    }
}