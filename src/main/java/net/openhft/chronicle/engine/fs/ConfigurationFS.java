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

import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.map.AuthenticatedKeyValueStore;
import net.openhft.chronicle.engine.map.FilePerKeyValueStore;
import net.openhft.chronicle.engine.tree.VanillaAsset;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

/**
 * Created by peter on 12/06/15.
 */
@Deprecated
public class ConfigurationFS implements MountPoint {
    public static final String FSTAB = "fstab.yaml";
    public final static String CLUSTERS = "clusters.yaml";
    private final String assetName;
    private final String etcDir;
    private final String baseDir;
    private AssetTree assetTree;

    public ConfigurationFS(String assetName, String etcDir, String baseDir) {
        this.assetName = assetName;
        this.etcDir = etcDir;
        this.baseDir = baseDir;
    }

    public void install(String baseDir, @NotNull AssetTree assetTree) {
        @NotNull Asset asset = assetTree.acquireAsset(assetName);

        if (asset.getView(MapView.class) == null) {
            ((VanillaAsset) asset).enableTranslatingValuesToBytesStore();
            @NotNull RequestContext context = RequestContext.requestContext(assetName)
                    .keyType(String.class).valueType(String.class);
            asset.registerView(AuthenticatedKeyValueStore.class, new FilePerKeyValueStore(context.basePath(etcDir), asset));
            asset.acquireView(MapView.class, context);
        }

        subscribeTo(assetTree);
    }

    public void subscribeTo(@NotNull AssetTree assetTree) {
        this.assetTree = assetTree;
        Subscriber<MapEvent> eventSub = this::onFile;
        assetTree.registerSubscriber(assetName, MapEvent.class, eventSub);
    }

    private void onFile(@NotNull MapEvent<String, String> mapEvent) {
        switch (mapEvent.getKey()) {
            case FSTAB:
                processFstab(mapEvent.getValue());
                break;
            case CLUSTERS:
                processClusters(mapEvent.getValue());
                break;

        }
    }

    private void processClusters(@NotNull String value) {
        @NotNull Clusters clusters = new Clusters();
        clusters.readMarshallable(TextWire.from(value));
        clusters.install(assetTree);
    }

    private void processFstab(@NotNull String value) {
        @NotNull Fstab fstab = new Fstab();
        fstab.readMarshallable(TextWire.from(value));
        fstab.install(baseDir, assetTree);
    }

    @Override
    public void readMarshallable(WireIn wire) throws IllegalStateException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public String spec() {
        return etcDir;
    }

    @Override
    public String name() {
        return assetName;
    }
}
