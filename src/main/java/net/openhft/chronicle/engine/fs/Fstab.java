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
    private final Map<String, MountPoint> mounts = new ConcurrentSkipListMap<>();

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
