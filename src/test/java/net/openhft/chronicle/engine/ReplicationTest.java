package net.openhft.chronicle.engine;

import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Publisher;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import org.junit.Test;

/**
 * Created by Rob Austin
 */
public class ReplicationTest {

    @Test
    public void testName() throws Exception {

        class MyEvent {
            String message;

            MyEvent(final String message) {
                this.message = message;
            }
        }

        final VanillaAssetTree assetTree = new VanillaAssetTree().forTesting();

        assetTree.registerSubscriber("rob", MyEvent.class, new Subscriber<MyEvent>() {

            @Override
            public void onMessage(final MyEvent myEvent) throws InvalidSubscriberException {
                System.out.println(myEvent.message);
            }

        });

        final Publisher<MyEvent> publisher = assetTree.acquirePublisher("rob", MyEvent.class);

        publisher.publish(new MyEvent("hello"));

    }
}
