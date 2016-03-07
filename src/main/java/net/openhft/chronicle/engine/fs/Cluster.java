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
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by peter.lawrey on 17/06/2015.
 */
public class Cluster implements Marshallable, Closeable {
    @NotNull
    private final Map<String, HostDetails> map;
    private final String clusterName;
    private final Map<InetSocketAddress, TcpHandler> tcpHandlers = new ConcurrentHashMap<>();

    public Cluster(String clusterName) {
        this.clusterName = clusterName;
        map = new ConcurrentSkipListMap<>();
    }

    public Cluster(String clusterName, Map<String, HostDetails> hostDetailsMap) {
        this.clusterName = clusterName;
        map = new ConcurrentSkipListMap<>(hostDetailsMap);
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        map.clear();
        StringBuilder hostDescription = new StringBuilder();
        while (wire.hasMore()) {
            wire.readEventName(hostDescription).marshallable(details -> {
                HostDetails hd = new HostDetails();
                hd.readMarshallable(details);
                map.put(hostDescription.toString(), hd);
            });
        }
    }

    public String clusterName() {
        return clusterName;
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        for (Entry<String, HostDetails> entry2 : map.entrySet()) {
            wire.writeEventName(entry2::getKey).marshallable(entry2.getValue());
        }
    }

    @NotNull
    public Collection<HostDetails> hostDetails() {
        return map.values();
    }

    @Override
    public void close() {
        hostDetails().forEach(Closeable::closeQuietly);
    }

    @Override
    public void notifyClosing() {
        hostDetails().forEach(HostDetails::notifyClosing);
    }


}
