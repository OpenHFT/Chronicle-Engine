/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.engine.Utils.methodName;

/**
 * @author Rob Austin.
 */

/**
 * test using the listener both remotely or locally via the engine
 *
 * @author Rob Austin.
 */
@RunWith(value = Parameterized.class)
public class RemoteSubscriptionTest extends ThreadMonitoringTest {

    private static final String NAME = "test";
    private static MapView<String, String> map;
    private static AtomicReference<Throwable> t = new AtomicReference();
    private final WireType wireType;
    public String connection;
    @NotNull
    @Rule
    public TestName name = new TestName();
    private AssetTree clientAssetTree = new VanillaAssetTree().forTesting(x -> t.compareAndSet(null, x));
    private VanillaAssetTree serverAssetTree;
    private ServerEndpoint serverEndpoint;

    public RemoteSubscriptionTest(WireType wireType) {
        this.wireType = wireType;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        final List<Object[]> list = new ArrayList<>();
        list.add(new Object[]{WireType.BINARY});
        list.add(new Object[]{WireType.TEXT});
        return list;
    }

    @After
    public void afterMethod() {
        final Throwable th = t.getAndSet(null);
        if (th != null) throw Jvm.rethrow(th);
    }

    @Before
    public void before() throws IOException {
        serverAssetTree = new VanillaAssetTree().forTesting(x -> t.compareAndSet(null, x));

        methodName(name.getMethodName());

        YamlLogging.showServerWrites(true);
        YamlLogging.showServerReads(true);

        connection = "StreamTest." + name.getMethodName() + ".host.port" + wireType;
        TCPRegistry.createServerSocketChannelFor(connection);
        serverEndpoint = new ServerEndpoint(connection, serverAssetTree);
        clientAssetTree = new VanillaAssetTree().forRemoteAccess(connection, wireType, x -> t.set(x));

        map = clientAssetTree.acquireMap(NAME, String.class, String.class);
    }

    @After
    public void after() throws IOException {
        clientAssetTree.close();
        Jvm.pause(1000);
        if (serverEndpoint != null)
            serverEndpoint.close();
        serverAssetTree.close();
        if (map instanceof Closeable)
            ((Closeable) map).close();
        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }

    /**
     * doing a put on the server, listening for the event on the client
     *
     * @throws Exception
     */
    @Test
    public void putServerListenOnClient() throws Exception {

        final MapView<String, String> serverMap = serverAssetTree.acquireMap("name", String.class, String
                .class);

        final BlockingQueue<MapEvent> events = new ArrayBlockingQueue<>(128);
        clientAssetTree.registerSubscriber("name", MapEvent.class, events::add);

        serverMap.put("hello", "world");

        final MapEvent event = events.poll(10, SECONDS);

        Assert.assertTrue(event instanceof InsertedEvent);

    }

    /**
     * doing a put on the client, listening for the event on the server
     *
     * @throws Exception
     */
    @Test
    public void putClientListenOnServer() throws Exception {

        final MapView<String, String> clientMap = clientAssetTree.acquireMap("name", String.class, String
                .class);

        final BlockingQueue<MapEvent> events = new ArrayBlockingQueue<>(128);
        serverAssetTree.registerSubscriber("name", MapEvent.class, events::add);

        clientMap.put("hello", "world");

        final MapEvent event = events.poll(10, SECONDS);

        Assert.assertTrue(event instanceof InsertedEvent);

    }

    /**
     * doing a put on the client, listening for the event on the server
     *
     * @throws Exception
     */
    @Test
    public void putClientListenOnClient() throws Exception {

        final MapView<String, String> clientMap = clientAssetTree.acquireMap("name", String.class, String
                .class);

        final BlockingQueue<MapEvent> events = new ArrayBlockingQueue<>(128);
        clientAssetTree.registerSubscriber("name?putReturnsNull=true", MapEvent.class,
                events::add);
        {
            clientMap.put("hello", "world");

            final MapEvent event = events.poll(10, SECONDS);

            Assert.assertTrue(event instanceof InsertedEvent);
        }
        {
            clientMap.put("hello", "world2");

            final MapEvent event = events.poll(10, SECONDS);

            Assert.assertTrue(event instanceof UpdatedEvent);
        }
    }

    /**
     * doing a put on the client, listening for the event on the server
     *
     * @throws Exception
     */
    @Test
    public void testEndOfSubscription() throws Exception {

        BlockingQueue<Boolean> endSub = new ArrayBlockingQueue<>(1);

        final Subscriber<MapEvent> eventHandler = new Subscriber<MapEvent>() {

            @Override
            public void onMessage(MapEvent mapEvent) {
                // do nothing
            }

            @Override
            public void onEndOfSubscription() {
                endSub.add(Boolean.TRUE);
            }
        };

        Thread.sleep(100);
        clientAssetTree.registerSubscriber("name", MapEvent.class, eventHandler);
        Thread.sleep(100);
        clientAssetTree.unregisterSubscriber("name", eventHandler);

        final Boolean onEndOfSubscription = endSub.poll(20, SECONDS);

        Assert.assertTrue(onEndOfSubscription);
    }

}

