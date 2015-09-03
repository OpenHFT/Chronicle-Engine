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
    static final int KEYS = Integer.getInteger("keys", 1000);
    static final int VALUE_LENGTH = Integer.getInteger("value.length", 8);

    public static void main(String[] args) {
        AssetTree clientAssetTree = new VanillaAssetTree().
                forRemoteAccess("localhost:8088", WireType.BINARY);
        Map<String, String> diskMap = clientAssetTree.acquireMap("/data/disk", String.class, String.class);

        long start = System.nanoTime();
        long total = 0;
        for (int i = 0; i < KEYS; i++) {
            StringBuilder sb = new StringBuilder();
            int count = 0;
            while (sb.length() < VALUE_LENGTH) {
                sb.append("value,").append(i).append(",").append(count++).append("\n");
            }
            total += sb.length();
            diskMap.put("key-" + i, sb.toString());
        }
        System.out.println("Keys.size: " + diskMap.size());
        long time = System.nanoTime() - start;
        System.out.printf("Took %.1f seconds to write %,d keys avg size: %,d bytes at %,d micro-seconds each%n",
                time / 1e9, KEYS, total / KEYS, time / KEYS / 1000);
    }
}
