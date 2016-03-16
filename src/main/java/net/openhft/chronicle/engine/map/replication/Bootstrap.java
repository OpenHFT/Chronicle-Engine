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

