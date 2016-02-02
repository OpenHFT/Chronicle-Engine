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
    public static final String CLUSTERS = "clusters.yaml";
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
        Asset asset = assetTree.acquireAsset(assetName);

        if (asset.getView(MapView.class) == null) {
            ((VanillaAsset) asset).enableTranslatingValuesToBytesStore();
            RequestContext context = RequestContext.requestContext(assetName)
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
        Clusters clusters = new Clusters();
        clusters.readMarshallable(TextWire.from(value));
        clusters.install(assetTree);
    }

    private void processFstab(@NotNull String value) {
        Fstab fstab = new Fstab();
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
