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
    private final Map<String, EngineCluster> clusterMap;

    public Clusters() {
        this.clusterMap = new ConcurrentSkipListMap<>();
    }

    public Clusters(Map<String, EngineCluster> clusterMap) {
        this.clusterMap = clusterMap;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        StringBuilder clusterName = Wires.acquireStringBuilder();
        while (wire.hasMore()) {
            wire.readEventName(clusterName).marshallable(host -> {
                EngineCluster engineCluster = clusterMap.computeIfAbsent(clusterName.toString(), EngineCluster::new);
                engineCluster.readMarshallable(host);
            });
        }
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        for (Entry<String, EngineCluster> entry : clusterMap.entrySet())
            wire.writeEventName(entry::getKey).marshallable(entry.getValue());
    }

    @Override
    public boolean equals(Object o) {
        return Marshallable.$equals(this, o);
    }

    @Override
    public int hashCode() {
        return Marshallable.$hashCode(this);
    }

    @Override
    public String toString() {
        return Marshallable.$toString(this);
    }

    public void install(@NotNull AssetTree assetTree) {
        final Asset root = assetTree.root();
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
}
