package net.openhft.chronicle.engine.fs;

import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by peter on 12/06/15.
 */
public class Fstab implements Marshallable {
    private List<MountPoint> mounts = new ArrayList<>();

    @Override
    public void readMarshallable(WireIn wire) throws IllegalStateException {
        wire.read(() -> "mounts").sequence(valueIn -> {
            while (valueIn.hasNextSequenceItem())
                mounts.add((MountPoint) valueIn.typedMarshallable());
        });
    }

    @Override
    public void writeMarshallable(WireOut wire) {
        wire.write(() -> "mounts").sequence(valueOut -> {
            for (MountPoint mount : mounts) {
                valueOut.typedMarshallable(mount);
            }
        });
    }

    public void install(AssetTree assetTree) {
        mounts.forEach(mp -> mp.install(assetTree));
    }
}
