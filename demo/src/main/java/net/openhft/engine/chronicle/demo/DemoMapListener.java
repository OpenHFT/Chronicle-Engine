package net.openhft.engine.chronicle.demo;

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

        //Create 3 types of subscribers
        TopicSubscriber<String, String> topicSubscriber = (topic, message) -> System.out.println("");

        Subscriber<MapEvent> mapEventSubscriber = e -> e.apply(mapEventListener);

        Subscriber keySubscriber = new Subscriber<String>() {
            @Override
            public void onMessage(String s) throws InvalidSubscriberException {
                System.out.println("key sub for " + s);
            }
        };

        //Initial registration
        clientAssetTree.registerSubscriber("/data/map", String.class, keySubscriber);
        clientAssetTree.registerSubscriber("/data/map", MapEvent.class, mapEventSubscriber);
        clientAssetTree.registerTopicSubscriber("/data/map", String.class, String.class, topicSubscriber);

        Scanner scanner = new Scanner(System.in);
        while(true){
            String line = scanner.nextLine();
            if(line.equalsIgnoreCase("quit"))System.exit(0);
            else if(line.equalsIgnoreCase("unregister subscriber")){
                System.out.println("Unregistering subscriber");
                clientAssetTree.unregisterSubscriber("/data/map", mapEventSubscriber);
            }else if(line.equalsIgnoreCase("unregister keySubscriber")){
                System.out.println("Unregistering key subscriber");
                clientAssetTree.unregisterSubscriber("/data/map", keySubscriber);
            }else if(line.equalsIgnoreCase("unregister topicSubscriber")){
                System.out.println("Unregistering topic subscriber");
                clientAssetTree.unregisterTopicSubscriber("/data/map", topicSubscriber);
            }else if(line.equalsIgnoreCase("register subscriber")){
                System.out.println("Registering subscriber");
                clientAssetTree.registerSubscriber("/data/map", MapEvent.class, mapEventSubscriber);
            }else if(line.equalsIgnoreCase("register keySubscriber")){
                System.out.println("Registering key subscriber");
                clientAssetTree.registerSubscriber("/data/map", String.class, keySubscriber);
            }else if(line.equalsIgnoreCase("register topicSubscriber")){
                System.out.println("Registering topic subscriber");
                clientAssetTree.registerTopicSubscriber("/data/map", String.class, String.class, topicSubscriber);
            }
        }
    }
}
