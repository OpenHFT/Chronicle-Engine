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

package net.openhft.chronicle.engine.cfg;

import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by peter on 26/08/15.
 */
public class ServerCfg implements Installable, Marshallable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerCfg.class);
    private int port;
    private WireType wireType;
    private boolean dumpWhenInDebug;
    private ServerEndpoint serverEndpoint;
    private int heartbeatIntervalTicks, heartbeatIntervalTimeout;

    @Override
    public ServerCfg install(String path, AssetTree assetTree) throws IOException {
        LOGGER.info(path + ": Starting listener on port " + port);
        serverEndpoint = new ServerEndpoint("*:" + port, assetTree, wireType, heartbeatIntervalTicks, heartbeatIntervalTimeout);
        if (dumpWhenInDebug)
            YamlLogging.setAll(true);
        return this;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        wire.read(() -> "wireType").asEnum(WireType.class, wt -> wireType = wt);
        wire.read(() -> "port").int32(this, (o, i) -> o.port = i);
        wire.read(() -> "dumpWhenInDebug").bool(this, (o, b) -> o.dumpWhenInDebug = b);
        wire.read(() -> "heartbeatIntervalTicks").int32(this, (o, i) -> o.heartbeatIntervalTicks = i);
        wire.read(() -> "heartbeatIntervalTimeout").int32(this, (o, i) -> o.heartbeatIntervalTimeout = i);
    }

    @Override
    public void writeMarshallable(WireOut wire) {
        wire.write(() -> "wireType").asEnum(wireType);
        wire.write(() -> "port").int32(port);
        wire.write(() -> "dumpWhenInDebug").bool(dumpWhenInDebug);
        wire.write(() -> "heartbeatIntervalTicks").int32(heartbeatIntervalTicks);
        wire.write(() -> "heartbeatIntervalTimeout").int32(heartbeatIntervalTimeout);
    }

    @Override
    public String toString() {
        return "ServerCfg{" +
                "port=" + port +
                ", wireType=" + wireType +
                ", dumpWhenInDebug=" + dumpWhenInDebug +
                ", serverEndpoint=" + serverEndpoint +
                ", heartbeatIntervalTicks=" + heartbeatIntervalTicks +
                ", heartbeatIntervalTimeout=" + heartbeatIntervalTimeout +
                '}';
    }
}
