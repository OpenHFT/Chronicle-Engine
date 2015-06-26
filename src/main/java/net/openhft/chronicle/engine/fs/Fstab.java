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

package net.openhft.chronicle.engine.fs;

import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by peter on 12/06/15.
 */
public class Fstab implements Marshallable {
    @NotNull
    private Map<String, MountPoint> mounts = new ConcurrentSkipListMap<>();

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        StringBuilder mountDesc = new StringBuilder();

        while (wire.hasMore()) {
            MountPoint mp = wire.readEventName(mountDesc).typedMarshallable();
            mounts.put(mountDesc.toString(), mp);
        }
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        for (Entry<String, MountPoint> entry : mounts.entrySet())
            wire.writeEventName(entry::getKey).typedMarshallable(entry.getValue());
    }

    public void install(String baseDir, AssetTree assetTree) {
        mounts.values().forEach(mp -> mp.install(baseDir, assetTree));
    }
}
