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

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by peter.lawrey on 17/06/2015.
 */
public class Clusters extends AbstractMarshallable implements Marshallable, Closeable {
    private final Map<String, EngineCluster> clusterMap = new ConcurrentSkipListMap<>();

    public Clusters() {

    }

    public Clusters(@NotNull Map<String, EngineCluster> clusterMap) {
        this.clusterMap.putAll(clusterMap);
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        StringBuilder clusterName = Wires.acquireStringBuilder();
        while (!wire.isEmpty()) {
            wire.readEventName(clusterName).marshallable(host -> {
                EngineCluster engineCluster = clusterMap.computeIfAbsent(clusterName.toString(), EngineCluster::new);
                engineCluster.readMarshallable(host);
            });
        }
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        for (@NotNull Entry<String, EngineCluster> entry : clusterMap.entrySet())
            wire.writeEventName(entry::getKey).marshallable(entry.getValue());
    }

    public void install(@NotNull AssetTree assetTree) {
        @NotNull final Asset root = assetTree.root();
        root.addView(Clusters.class, this);

        if (clusterMap == null)
            return;
        clusterMap.values().forEach(v -> {
            v.assetRoot(root);
            v.install();
        });

    }

    public EngineCluster get(String cluster) {
        return clusterMap.get(cluster);
    }

    public EngineCluster firstCluster() {
        return clusterMap.values().iterator().next();
    }

    public void put(String clusterName, EngineCluster engineCluster) {
        clusterMap.put(clusterName, engineCluster);
    }

    @Override
    public void close() {
        clusterMap.values().forEach(Closeable::closeQuietly);
    }

    @Override
    public void notifyClosing() {
        clusterMap.values().forEach(EngineCluster::notifyClosing);
    }

    /**
     * @return the number of clusters
     */
    public int size() {
        return clusterMap.size();
    }
}
