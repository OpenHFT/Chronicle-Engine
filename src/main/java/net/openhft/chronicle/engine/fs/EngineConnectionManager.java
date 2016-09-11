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

import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.cluster.ConnectionManager;
import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Rob Austin.
 */
public class EngineConnectionManager implements ConnectionManager {

    private final Set<ConnectionListener> connectionListeners = Collections.newSetFromMap(new
            IdentityHashMap<>());

    private ConcurrentHashMap<NetworkContext, AtomicBoolean> isConnected = new
            ConcurrentHashMap<>();

    private EngineConnectionManager() {

    }

    @Override
    public synchronized void addListener(ConnectionListener connectionListener) {

        connectionListeners.add(connectionListener);

        isConnected.forEach((wireOutPublisher, connected) ->
                connectionListener.onConnectionChange(wireOutPublisher, connected.get()));
    }

    @Override
    public synchronized void onConnectionChanged(boolean isConnected, final NetworkContext nc) {

        final Function<NetworkContext, AtomicBoolean> f = v -> new AtomicBoolean();
        boolean wasConnected = this.isConnected.computeIfAbsent(nc, f).getAndSet
                (isConnected);
        if (wasConnected != isConnected)
            connectionListeners.forEach(l -> l.onConnectionChange(nc, isConnected));
    }

    public static class Factory implements
            Supplier<ConnectionManager>,
            Demarshallable,
            WriteMarshallable {

        @UsedViaReflection
        private Factory(@NotNull WireIn wireIn) {
        }

        public Factory() {
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {

        }

        @Override
        public EngineConnectionManager get() {
            return new EngineConnectionManager();
        }
    }
}
