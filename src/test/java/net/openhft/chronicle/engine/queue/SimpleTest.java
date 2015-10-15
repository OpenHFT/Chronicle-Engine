package net.openhft.chronicle.engine.queue;

import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.pubsub.Publisher;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.engine.Chassis.acquirePublisher;
import static net.openhft.chronicle.engine.Chassis.registerSubscriber;
import static net.openhft.chronicle.engine.Utils.methodName;
import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
@RunWith(value = Parameterized.class)
public class SimpleTest extends ThreadMonitoringTest {


    private static final String NAME = "/test";

    private final boolean isRemote;
    @NotNull
    @Rule
    public TestName name = new TestName();

    public SimpleTest(Boolean isRemote) {
        this.isRemote = isRemote;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() throws IOException {

        return Arrays.asList(new Boolean[][]{
                {false}
                // ,{true}
        });
    }

    @AfterClass
    public static void tearDownClass() {
        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }

    @Before
    public void before() {
        methodName(name.getMethodName());
    }


    class MyMarshallable implements Marshallable {

        String s;

        public MyMarshallable(String s) {
            this.s = s;
        }

        @Override
        public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
            s = wire.read().text();
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            wire.write().text(s);
        }

        @Override
        public String toString() {
            return s;
        }
    }

    @Test
    public void publishToATopic() throws InterruptedException {

        Publisher<MyMarshallable> publisher = acquirePublisher("/queue", MyMarshallable.class);
        BlockingQueue<MyMarshallable> values = new ArrayBlockingQueue<>(1);
        Subscriber<MyMarshallable> subscriber = values::add;
        registerSubscriber("/queue", MyMarshallable.class, subscriber);

        publisher.publish(new MyMarshallable("Message-1"));
        assertEquals("Message-1", values.poll(2, SECONDS).toString());

        // Publisher<MyMarshallable> publisher = acquireTopicPublisher("/queue", MyMarshallable
        //      .class);

    }


}

