package net.openhft.chronicle.engine;

import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Publisher;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import org.junit.Test;

import java.util.concurrent.ConcurrentMap;

/**
 * Created by Rob Austin
 */
public class ReplicationTest {

    @Test
    public void testName() throws Exception {

        class ReplicationEvent {
            String message;

            ReplicationEvent(final String message) {
                this.message = message;
            }
        }

        final VanillaAssetTree assetTree = new VanillaAssetTree().forTesting();

        final ConcurrentMap<String, String> rob = assetTree.acquireMap("rob", String.class, String.class);

        assetTree.registerSubscriber("rob", ReplicationEvent.class, new Subscriber<ReplicationEvent>() {

            @Override
            public void onMessage(final ReplicationEvent myEvent) throws InvalidSubscriberException {
                System.out.println(myEvent.message);
            }

        });

        final Publisher<ReplicationEvent> publisher = assetTree.acquirePublisher("rob", ReplicationEvent.class);
        publisher.publish(new ReplicationEvent("hello"));

    }
}
