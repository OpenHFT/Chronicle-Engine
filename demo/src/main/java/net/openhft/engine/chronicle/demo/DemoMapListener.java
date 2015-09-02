package net.openhft.engine.chronicle.demo;

import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapEventListener;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.WireType;
import net.openhft.engine.chronicle.demo.data.EndOfDayShort;

import java.util.Map;
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
                System.out.println("Update received " +  key + ":" + newValue);
            }

            @Override
            public void insert(String assetName, String key, String value) {
                System.out.println("Insert received " + key + ":" + value);
            }

            @Override
            public void remove(String assetName, String key, String value) {
                System.out.println("Remove received " + key + ":" + value);
            }
        };
        Subscriber<MapEvent> mapEventSubscriber = e -> e.apply(mapEventListener);
        clientAssetTree.registerSubscriber("/data/map", MapEvent.class, mapEventSubscriber);

        Scanner scanner = new Scanner(System.in);
        while(true){
            String line = scanner.nextLine();
            if(line.equalsIgnoreCase("quit"))System.exit(0);
        }
    }
}
