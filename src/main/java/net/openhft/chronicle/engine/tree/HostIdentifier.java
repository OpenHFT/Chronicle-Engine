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

package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.engine.api.tree.Asset;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Created by peter.lawrey on 15/06/2015.
 */
public class HostIdentifier {
    private final byte hostId;

    public HostIdentifier(byte hostId) {
        this.hostId = hostId;
    }

    public static byte localIdentifier(@NotNull Asset asset) {
        @Nullable HostIdentifier hostIdentifier = asset.findOrCreateView(HostIdentifier.class);
        if (hostIdentifier == null)
            return 0;

        return hostIdentifier.hostId();
    }

    public byte hostId() {
        return hostId;
    }

    @NotNull
    @Override
    public String toString() {
        return "HostIdentifier{" +
                "hostId=" + hostId +
                '}';
    }
}
