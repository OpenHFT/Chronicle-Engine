/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.Chassis;
import net.openhft.chronicle.engine.Factor;
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.map.ChangeEvent;
import net.openhft.chronicle.engine.api.map.MapEventListener;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.wire.TextWire;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static net.openhft.chronicle.engine.Utils.yamlLoggger;
import static net.openhft.chronicle.engine.api.WireType.wire;
import static net.openhft.chronicle.engine.map.MapClientTest.RemoteMapSupplier.toUri;
import static org.junit.Assert.assertEquals;

/**
 * test using the listener both remotely or locally via the engine
 *
 * @author Rob Austin.
 */
@RunWith(value = Parameterized.class)
public class SubscriptionTest extends ThreadMonitoringTest {
    private final AtomicInteger success = new AtomicInteger();
    private int port;
    private ConcurrentMap<String, Factor> map;

    @Parameterized.Parameters
    public static Collection<Object[]> data() throws IOException {

        return Arrays.asList(new Boolean[][]{
                {true},
                {false}
        });
    }

    public SubscriptionTest(Boolean isRemote) throws IOException {

        MapEventListener<String, Factor> listener = new MapEventListener<String, Factor>() {
            @Override
            public void update(String key, Factor oldValue, Factor newValue) {
                System.out.println("Updated { key: " + key + ", oldValue: " + oldValue + ", value: " + newValue + " }");
                success.set(-1000);
            }

            @Override
            public void insert(String key, Factor value) {
                System.out.println("Inserted { key: " + key + ", value: " + value + " }");
                success.incrementAndGet();
            }

            @Override
            public void remove(String key, Factor oldValue) {
                System.out.println("Removed { key: " + key + ", value: " + oldValue + " }");
                success.set(-100);
            }
        };

        if (isRemote) {
            Chassis.resetChassis();
            wire = TextWire::new;

            ServerEndpoint serverEndpoint = new ServerEndpoint(Chassis.defaultSession());
            port = serverEndpoint.getPort();
            Chassis.forRemoteAccess();
            map = Chassis.acquireMap(toUri(port, "localhost"), String.class, Factor.class);
            Chassis.registerSubscriber(toUri(port, "localhost"), ChangeEvent.class, e -> e.apply(listener));
        } else {
            Chassis.resetChassis();
            map = Chassis.acquireMap("TEST", String.class, Factor.class);
            Chassis.registerSubscriber("TEST", ChangeEvent.class, e -> e.apply(listener));
        }
    }

    @Test
    public void testSubscriptionTest() throws IOException, InterruptedException {
        yamlLoggger(() -> {
            Factor factor = new Factor();
            factor.setAccountNumber("xyz");
            map.put("testA", factor);
            assertEquals(1, map.size());
            assertEquals("xyz", map.get("testA").getAccountNumber());

            expectedSuccess(success, 1);

            factor = new Factor();
            factor.setAccountNumber("abc");
            map.put("testB", factor);
            assertEquals("abc", map.get("testB").getAccountNumber());

            expectedSuccess(success, 2);
            success.set(0);

            //Changing factor account name from xyz to abc
            factor = new Factor();
            factor.setAccountNumber("ddd");
            map.put("testA", factor);
            assertEquals("ddd", map.get("testA").getAccountNumber());

            expectedSuccess(success, -1000);
            success.set(0);

            map.remove("testA");
            expectedSuccess(success, -100);
        });
    }

    private void expectedSuccess(@NotNull AtomicInteger success, int expected) {
        for (int i = 0; i < 20; i++) {
            if (success.get() == expected)
                break;
            try {
                TimeUnit.MILLISECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        assertEquals(expected, success.get());
    }
}

