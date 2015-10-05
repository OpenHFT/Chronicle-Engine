package net.openhft.engine.chronicle.demo;

import net.openhft.chronicle.engine.api.pubsub.Publisher;
import net.openhft.chronicle.engine.api.pubsub.TopicPublisher;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.WireType;

/**
 * Created by daniel on 25/09/2015.
 */
public class DemoMapPublisher {
    public static void main(String[] args) {
        AssetTree clientAssetTree = new VanillaAssetTree().
                forRemoteAccess("localhost:8088", WireType.BINARY);
        //Creates a VanillaMap underneath to publish into.
        Publisher<String> publisher = clientAssetTree.acquirePublisher("/data/map3", String.class);
        publisher.publish("test2");

//        TopicPublisher<String, String> topicPublisher =
//                clientAssetTree.acquireTopicPublisher("/data/map37", String.class, String.class);
//        topicPublisher.publish("test4", "topic");
//        topicPublisher.publish("test5", "topic");
//        topicPublisher.publish("test5", "topic3");


    }
}
