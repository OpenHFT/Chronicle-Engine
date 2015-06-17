package net.openhft.chronicle.engine.fs;

import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by peter on 12/06/15.
 */
public class Fstab implements Marshallable {
    private Map<String, MountPoint> mounts = new ConcurrentSkipListMap<>();

    @Override
    public void readMarshallable(WireIn wire) throws IllegalStateException {
        StringBuilder mountDesc = new StringBuilder();

        while (wire.hasMore()) {
            MountPoint mp = wire.readEventName(mountDesc).typedMarshallable();
            mounts.put(mountDesc.toString(), mp);
        }
    }

    @Override
    public void writeMarshallable(WireOut wire) {
        for (Map.Entry<String, MountPoint> entry : mounts.entrySet())
            wire.writeEventName(entry::getKey).typedMarshallable(entry.getValue());
    }

    public void install(String baseDir, AssetTree assetTree) {
        mounts.values().forEach(mp -> mp.install(baseDir, assetTree));
    }
}
