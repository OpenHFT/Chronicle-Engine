package net.openhft.engine.chronicle.demo;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapEventListener;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.WireType;

import java.util.Scanner;

/**
 * Created by daniel on 02/09/2015.
 * This demo program created a listener to the /data/map.
 */
public class DemoMapListener {
    public static void main(String[] args) {
        AssetTree clientAssetTree = new VanillaAssetTree().
                forRemoteAccess("localhost:8088", WireType.BINARY);

        MapEventListener<String, String> mapEventListener = new MapEventListener<String, String>() {
            @Override
            public void update(String assetName, String key, String oldValue, String newValue) {
                System.out.println("Update received for " + assetName +  " " + key + ":" + newValue);
            }

            @Override
            public void insert(String assetName, String key, String value) {
                System.out.println("Insert received for " + assetName +  " "  + key + ":" + value);
            }

            @Override
            public void remove(String assetName, String key, String value) {
                System.out.println("Remove received for " + assetName +  " "  + key + ":" + value);
            }
        };
        Subscriber<MapEvent> mapEventSubscriber = e -> e.apply(mapEventListener);
        clientAssetTree.registerSubscriber("/data/map", MapEvent.class, mapEventSubscriber);

        Jvm.pause(1000);

        clientAssetTree.unregisterSubscriber("/data/map", mapEventSubscriber);

        clientAssetTree.registerSubscriber("/data/map", String.class, s -> {
            System.out.println("key sub for " + s);
        });

        clientAssetTree.registerTopicSubscriber("/data/map", String.class, String.class, new TopicSubscriber<String, String>() {
            @Override
            public void onMessage(String topic, String message) throws InvalidSubscriberException {
                System.out.println("");
            }
        });

        Scanner scanner = new Scanner(System.in);
        while(true){
            String line = scanner.nextLine();
            if(line.equalsIgnoreCase("quit"))System.exit(0);
        }
    }
}
