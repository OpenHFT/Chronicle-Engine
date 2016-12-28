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
    private YamlLogging.YamlLoggingLevel logTCPMessages;
    private ServerEndpoint serverEndpoint;
    private int heartbeatIntervalTicks, heartbeatIntervalTimeout;

    @NotNull
    @Override
    public ServerCfg install(String path, @NotNull AssetTree assetTree) throws IOException {
        LOGGER.info(path + ": Starting listener on port " + port);
        serverEndpoint = new ServerEndpoint("*:" + port, assetTree);
       // YamlLogging.setAll(false);
        return this;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        wire.read(() -> "wireType").asEnum(WireType.class, wt -> wireType = wt);
        wire.read(() -> "port").int32(this, (o, i) -> o.port = i);
        wire.read(() -> "logTCPMessages").asEnum(YamlLogging.YamlLoggingLevel.class, this, (o, b) -> o.logTCPMessages = b);
        wire.read(() -> "heartbeatIntervalTicks").int32(this, (o, i) -> o.heartbeatIntervalTicks = i);
        wire.read(() -> "heartbeatIntervalTimeout").int32(this, (o, i) -> o.heartbeatIntervalTimeout = i);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(() -> "wireType").asEnum(wireType);
        wire.write(() -> "port").int32(port);
        wire.write(() -> "logTCPMessages").asEnum(logTCPMessages);
        wire.write(() -> "heartbeatIntervalTicks").int32(heartbeatIntervalTicks);
        wire.write(() -> "heartbeatIntervalTimeout").int32(heartbeatIntervalTimeout);
    }

    @NotNull
    @Override
    public String toString() {
        return "ServerCfg{" +
                "port=" + port +
                ", wireType=" + wireType +
                ", logTCPMessages=" + logTCPMessages +
                ", serverEndpoint=" + serverEndpoint +
                ", heartbeatIntervalTicks=" + heartbeatIntervalTicks +
                ", heartbeatIntervalTimeout=" + heartbeatIntervalTimeout +
                '}';
    }
}
