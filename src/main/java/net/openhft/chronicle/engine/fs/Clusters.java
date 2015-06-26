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
import net.openhft.chronicle.engine.api.tree.View;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by peter.lawrey on 17/06/2015.
 */
public class Clusters implements Marshallable, View {
    private final Map<String, Map<String, HostDetails>> clusterMap =
            new ConcurrentSkipListMap<>();

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        StringBuilder clusterName = new StringBuilder();
        StringBuilder hostDescription = new StringBuilder();
        while (wire.hasMore()) {
            wire.readEventName(clusterName).marshallable(host -> {
                Map<String, HostDetails> hdMap = clusterMap.computeIfAbsent(clusterName.toString(), k -> new ConcurrentSkipListMap<>());
                while (wire.hasMore()) {
                    host.readEventName(hostDescription).marshallable(details -> {
                        HostDetails hd = new HostDetails();
                        hd.readMarshallable(details);
                        hdMap.put(hostDescription.toString(), hd);
                    });
                }
            });
        }
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        for (Map.Entry<String, Map<String, HostDetails>> entry : clusterMap.entrySet()) {
            wire.writeEventName(entry::getKey).marshallable(host -> {
                for (Map.Entry<String, HostDetails> entry2 : entry.getValue().entrySet()) {
                    wire.writeEventName(entry2::getKey).marshallable(entry2.getValue());
                }
            });
        }
    }

    public void install(@NotNull AssetTree assetTree) {
        assetTree.root().addView(Clusters.class, this);
    }

    public Map<String, HostDetails> get(String cluster) {
        return clusterMap.get(cluster);
    }

    public void put(String cluster, Map<String, HostDetails> hostDetailsMap) {
        clusterMap.put(cluster, hostDetailsMap);
    }
}
