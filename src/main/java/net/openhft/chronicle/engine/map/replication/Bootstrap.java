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

package net.openhft.chronicle.engine.map.replication;

import net.openhft.chronicle.wire.AbstractMarshallable;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

/**
 * Created by Rob Austin
 */
public class Bootstrap extends AbstractMarshallable implements Marshallable {

    private byte identifier;

    private long lastUpdatedTime;

    public long lastUpdatedTime() {
        return lastUpdatedTime;
    }

    public void lastUpdatedTime(final long lastModificationTime) {
        this.lastUpdatedTime = lastModificationTime;
    }

    public void identifier(final byte identifier) {
        this.identifier = identifier;
    }

    public byte identifier() {
        return identifier;
    }

    @Override
    public void writeMarshallable(@NotNull final WireOut wire) {
        wire.write(() -> "id").int8(identifier);
        wire.write(() -> "lastUpdatedTime").int64(lastUpdatedTime);
    }

    @Override
    public void readMarshallable(@NotNull final WireIn wire) throws IllegalStateException {
        identifier = wire.read(() -> "id").int8();
        lastUpdatedTime = wire.read(() -> "lastUpdatedTime").int64();
    }
}

