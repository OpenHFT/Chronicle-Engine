package net.openhft.chronicle.engine.fs;

import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.api.tree.View;
import net.openhft.chronicle.wire.Marshallable;

/**
 * Created by peter on 12/06/15.
 */
public interface MountPoint extends Marshallable, View {
    String spec();

    String name();

    void install(AssetTree assetTree);
}
