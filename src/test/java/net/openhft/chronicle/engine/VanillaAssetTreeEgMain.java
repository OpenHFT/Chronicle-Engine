package net.openhft.chronicle.engine;

import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.tree.TopologicalEvent;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;

import java.util.concurrent.ConcurrentMap;

/**
 * Created by peter.lawrey on 16/06/2015.
 */
public class VanillaAssetTreeEgMain {
    public static void main(String[] args) {
        AssetTree tree = new VanillaAssetTree().forTesting();

        // start with some elements
        ConcurrentMap<String, String> map1 = tree.acquireMap("group/map1", String.class, String.class);
        map1.put("key1", "value1");

        ConcurrentMap<String, String> map2 = tree.acquireMap("group/map2", String.class, String.class);
        map2.put("key1", "value1");
        map2.put("key2", "value2");

        ConcurrentMap<String, String> map3 = tree.acquireMap("group2/subgroup/map3", String.class, String.class);
        map3.put("keyA", "value1");
        map3.put("keyB", "value1");
        map3.put("keyC", "value1");

        tree.registerSubscriber("", TopologicalEvent.class, e -> {
            if (e.added()) {
                System.out.println("Added a " + e.name() + " under " + e.assetName());
            } else {
                System.out.println("Removed a " + e.name() + " under " + e.assetName());
            }
        });

        // added group2/subgroup/map4
        ConcurrentMap<String, String> map4 = tree.acquireMap("group2/subgroup/map4", String.class, String.class);
        map4.put("keyA", "value1");
        map4.put("keyB", "value1");
        map4.put("keyC", "value1");

        // removed group/map2
        tree.root().getAsset("group").removeChild("map2");

    }
}
