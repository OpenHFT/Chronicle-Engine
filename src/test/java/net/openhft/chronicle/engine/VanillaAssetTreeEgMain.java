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

package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.management.ManagementTools;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.map.ObjectKeyValueStore;
import net.openhft.chronicle.engine.map.ObjectSubscription;
import net.openhft.chronicle.engine.tree.TopologicalEvent;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.lang.thread.NamedThreadFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by peter.lawrey on 16/06/2015.
 */
public class VanillaAssetTreeEgMain {
    static final ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("all-trees-watcher", true));

    public static void main(String[] args) {
        @NotNull AssetTree tree = new VanillaAssetTree().forTesting();
        tree.enableManagement(18181);

        // start with some elements
        @NotNull ConcurrentMap<String, String> map1 = tree.acquireMap("group/map1", String.class, String.class);
        map1.put("key1", "ABCDEGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz0132456789ABCDEGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz0132456789");
        map1.put("key2", "ABCDEGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz0132456789ABCDEGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz0132456789");
        map1.put("key3", "ABCDEGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz0132456789ABCDEGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz0132456789");
        map1.put("key4", "ABCDEGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz0132456789ABCDEGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz0132456789");

        @NotNull ConcurrentMap<String, Integer> map2 = tree.acquireMap("group/map2", String.class, Integer.class);
        map2.put("key1", 11);
        map2.put("key2", 2222);
        //tree.registerSubscriber("group/map2", MapEvent.class, System.out::println);

        @NotNull ConcurrentMap<String, String> map3 = tree.acquireMap("group2/subgroup/map3", String.class, String.class);
        map3.put("keyA", "value1");
        map3.put("keyB", "value1");
        map3.put("keyC", "value1");
        //tree.registerSubscriber("group2/subgroup/map3", String.class, (String s) -> System.out.println(s));
        tree.registerSubscriber("group2/subgroup/map3", String.class, System.out::println);

        registerTextViewofTree("tree", tree);

        // added group2/subgroup/map4
        @NotNull ConcurrentMap<String, String> map4 = tree.acquireMap("group2/subgroup/map4", String.class, String.class);
        map4.put("4-keyA", "value1");
        map4.put("4-keyB", "value1");
        map4.put("4-keyC", "value1");
        /*tree.registerTopicSubscriber("group2/subgroup/map4", String.class, String.class,
                (k, v) -> System.out.println("key: " + k + ", value: " + v));*/
        // give the listener time to see the collection before we delete it.
        //Jvm.pause(200);

        // removed group/map2
        tree.root().getAsset("group").removeChild("map2");

        Jvm.pause(2 * 60 * 1000);

        for (int i = 0; i < 1000; i++) {
            map4.put("Key"+i,"Value"+i);
        }

        System.out.println("Here..............");

        @NotNull AssetTree tree1 = new VanillaAssetTree().forTesting();
        tree1.enableManagement();
        @NotNull ConcurrentMap<String, String> map_tree = tree1.acquireMap("group1/1/2/3/4/5/map", String.class, String.class);
        map_tree.put("key1", "ABCDEGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz0132456789ABCDEGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz0132456789");
        map_tree.put("key2", "ABCDEGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz0132456789ABCDEGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz0132456789");
        map_tree.put("key3", "ABCDEGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz0132456789ABCDEGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz0132456789");
        map_tree.put("key4", "ABCDEGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz0132456789ABCDEGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz0132456789");
        map_tree.put("key5", "ABCDEGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz0132456789ABCDEGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz0132456789");
        @NotNull ConcurrentMap<String, String> map_tree2 = tree1.acquireMap("group1/1/map", String.class, String.class);
        map_tree2.put("key1", "value1");
        //Jvm.pause(50000);
        tree1.root().getAsset("group1/1").removeChild("map");
        map_tree.remove("key4");
        System.out.println("count="+ ManagementTools.getCount());
        //tree.disableManagement();
        System.out.println("After disable tree count="+ ManagementTools.getCount());

        //Jvm.pause(2000);

    }

    public static void registerTextViewofTree(String desc, @NotNull AssetTree tree) {
        tree.registerSubscriber("", TopologicalEvent.class, e ->
                        // give the collection time to be setup.
                        ses.schedule(() -> handleTreeUpdate(desc, tree, e), 50, TimeUnit.MILLISECONDS)
        );
    }

    static void handleTreeUpdate(String desc, @NotNull AssetTree tree, @NotNull TopologicalEvent e) {
        try {
            System.out.println(desc + " handle " + e);
            if (e.added()) {
                System.out.println(desc + " Added a " + e.name() + " under " + e.assetName());
                @NotNull String assetFullName = e.fullName();
                @Nullable Asset asset = tree.getAsset(assetFullName);
                if (asset == null) {
                    System.out.println("\tbut it's not visible.");
                    return;
                }
                @Nullable ObjectKeyValueStore view = asset.getView(ObjectKeyValueStore.class);
                if (view == null) {
                    System.out.println("\t[node]");
                } else {
                    long elements = view.longSize();
                    Class keyType = view.keyType();
                    Class valueType = view.valueType();
                    @Nullable ObjectSubscription objectSubscription = asset.getView(ObjectSubscription.class);
                    int keySubscriberCount = objectSubscription.keySubscriberCount();
                    int entrySubscriberCount = objectSubscription.entrySubscriberCount();
                    int topicSubscriberCount = objectSubscription.topicSubscriberCount();
                    System.out.println("\t[map]");
                    System.out.printf("\t%-20s %s%n", "keyType", keyType.getName());
                    System.out.printf("\t%-20s %s%n", "valueType", valueType.getName());
                    System.out.printf("\t%-20s %s%n", "size", elements);
                    System.out.printf("\t%-20s %s%n", "keySubscriberCount", keySubscriberCount);
                    System.out.printf("\t%-20s %s%n", "entrySubscriberCount", entrySubscriberCount);
                    System.out.printf("\t%-20s %s%n", "topicSubscriberCount", topicSubscriberCount);
                }
            } else {
                System.out.println(desc + " Removed a " + e.name() + " under " + e.assetName());
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
