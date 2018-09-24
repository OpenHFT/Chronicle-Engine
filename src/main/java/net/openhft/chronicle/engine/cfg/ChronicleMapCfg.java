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

package net.openhft.chronicle.engine.cfg;

import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.map.ChronicleMapKeyValueStore;
import net.openhft.chronicle.engine.map.ObjectKeyValueStore;
import net.openhft.chronicle.engine.tree.VanillaAsset;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import software.chronicle.enterprise.map.config.ReplicatedMapCfg;

import java.util.Map;

public class ChronicleMapCfg<K, V> extends ReplicatedMapCfg<K, V> implements Installable {
    private Map<String, String> replicationHostMapping;

    public Map<String, String> replicationHostMapping() {
        return replicationHostMapping;
    }

    public void replicationHostMapping(Map<String, String> replicationHostMapping) {
        this.replicationHostMapping = replicationHostMapping;
    }

    @Nullable
    @Override
    public Void install(@NotNull String path, @NotNull AssetTree assetTree) {
        @NotNull Asset asset = assetTree.acquireAsset(path);
        ((VanillaAsset) asset).enableTranslatingValuesToBytesStore();
        name(asset.fullName());

        @NotNull ChronicleMapKeyValueStore<K, V> chronicleMapKeyValueStore = new ChronicleMapKeyValueStore<>(this, asset);
        asset.addView(ObjectKeyValueStore.class, chronicleMapKeyValueStore);

        return null;
    }

}
