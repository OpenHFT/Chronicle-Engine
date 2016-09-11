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

import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.EngineReplication.ModificationIterator;
import net.openhft.chronicle.engine.api.EngineReplication.ReplicationEntry;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.Replication;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Supplier;

/**
 * Created by Rob Austin
 */
public class VanillaReplication implements Replication {

    private final MapView mapView;

    public VanillaReplication(final RequestContext requestContext, final Asset asset, final MapView mapView) {
        this.mapView = mapView;
    }

    @Override
    public void applyReplication(@NotNull final ReplicationEntry replicatedEntry) {
        ((KeyValueStore) mapView.underlying()).accept(replicatedEntry);
    }

    @Nullable
    @Override
    public ModificationIterator acquireModificationIterator(final byte id) {
        EngineReplication engineReplication = ((Supplier<EngineReplication>) mapView.underlying()).get();
        return engineReplication.acquireModificationIterator(id);
    }

    @Override
    public long lastModificationTime(final byte id) {
        EngineReplication engineReplication = ((Supplier<EngineReplication>) mapView.underlying()).get();
        return engineReplication.lastModificationTime(id);
    }

    @Override
    public void setLastModificationTime(byte identifier, long timestamp) {
        EngineReplication engineReplication = ((Supplier<EngineReplication>) mapView.underlying()).get();
        engineReplication.setLastModificationTime(identifier, timestamp);
    }
}
