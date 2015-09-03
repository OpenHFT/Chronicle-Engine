package net.openhft.engine.chronicle.demo;

import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.WireType;

import java.util.Map;

/**
 * Created by daniel on 02/09/2015.
 * A very simple program to demonstrate how to change value.
 */
public class UpdateDiskDataMain {
    public static void main(String[] args) {
        AssetTree clientAssetTree = new VanillaAssetTree().
                forRemoteAccess("localhost:8088", WireType.BINARY);
        Map<String, String> diskMap = clientAssetTree.acquireMap("/data/disk", String.class, String.class);

        long start = System.nanoTime();
        int keys = 1000;
        for (int i = 0; i < keys; i++) {
            diskMap.put("key-" + i, "value-" + i);
        }
        System.out.println("Keys.size: " + diskMap.size());
        long time = System.nanoTime() - start;
        System.out.printf("Took %.1f seconds to write %,d keys, at %,d micro-seconds each%n",
                time / 1e9, keys, time / keys / 1000);
    }
}
