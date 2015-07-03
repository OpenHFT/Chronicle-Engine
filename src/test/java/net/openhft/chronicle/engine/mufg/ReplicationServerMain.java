package net.openhft.chronicle.engine.mufg;

import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.Publisher;
import net.openhft.chronicle.engine.api.pubsub.Replication;
import net.openhft.chronicle.engine.api.pubsub.TopicPublisher;
import net.openhft.chronicle.engine.api.session.SessionProvider;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.fs.Clusters;
import net.openhft.chronicle.engine.fs.HostDetails;
import net.openhft.chronicle.engine.map.*;
import net.openhft.chronicle.engine.pubsub.VanillaReference;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.session.VanillaSessionProvider;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.engine.tree.VanillaReplication;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.threads.api.EventLoop;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;

import java.io.IOException;
import java.util.HashMap;

import static net.openhft.chronicle.engine.api.tree.RequestContext.requestContext;

/**
 * Created by Rob Austin
 */

public class ReplicationServerMain {


    public static final String HOST = System.getProperty("remote.host");
    public static final Integer HOST_ID = Integer.getInteger("hostId", 1);

    public static void main(String[] args) throws IOException {
        YamlLogging.clientReads = true;
        YamlLogging.clientWrites = true;
        WireType wireType = WireType.TEXT;

        final Integer host = HOST_ID;

        System.out.println("using hostid=" + HOST_ID);
        System.out.println("using host=" + HOST);

        final VanillaAssetTree tree = new VanillaAssetTree(host);
        newCluster(host, tree);
        tree.root().addLeafRule(EngineReplication.class, "Engine replication holder",
                CMap2EngineReplicator::new);

        tree.root().addView(SessionProvider.class, new VanillaSessionProvider());
        tree.root().addWrappingRule(Replication.class, "replication", VanillaReplication::new, MapView.class);
        tree.root().addWrappingRule(MapView.class, "mapv view", VanillaMapView::new, AuthenticatedKeyValueStore.class);
        tree.root().addWrappingRule(TopicPublisher.class, " topic publisher", VanillaTopicPublisher::new, MapView.class);
        tree.root().addWrappingRule(Publisher.class, "publisher", VanillaReference::new, MapView.class);
        tree.root().addLeafRule(ObjectKVSSubscription.class, " vanilla", VanillaKVSSubscription::new);

        ThreadGroup threadGroup = new ThreadGroup("my-named-thread-group");
        tree.root().addView(ThreadGroup.class, threadGroup);

        tree.root().addView(EventLoop.class, new EventGroup(false));
        Asset asset = tree.root().acquireAsset("map");
        asset.addView(AuthenticatedKeyValueStore.class, new ChronicleMapKeyValueStore<>(requestContext("map"), asset));

        tree.root().addLeafRule(ObjectKVSSubscription.class, " ObjectKVSSubscription",
                VanillaKVSSubscription::new);


        new ServerEndpoint(5700 + host, tree, wireType);
    }


    private static void newCluster(Integer host, VanillaAssetTree tree) {
        Clusters clusters = new Clusters();
        HashMap<String, HostDetails> hostDetailsMap = new HashMap<String, HostDetails>();

        {
            final HostDetails value = new HostDetails();
            value.hostId = 1;
            value.hostname = host == 1 ? "localhost" : HOST;
            value.port = 5701;
            value.timeoutMs = 1000;
            hostDetailsMap.put("host1", value);
        }
        {
            final HostDetails value = new HostDetails();
            value.hostId = 2;
            value.hostname = host == 2 ? "localhost" : HOST;
            value.port = 5702;
            value.timeoutMs = 1000;
            hostDetailsMap.put("host2", value);
        }


        clusters.put("cluster", hostDetailsMap);
        tree.root().addView(Clusters.class, clusters);
    }
}

