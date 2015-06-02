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

import net.openhft.chronicle.engine.client.internal.ChronicleEngine;
import net.openhft.chronicle.engine.map.MapClientTest.RemoteMapSupplier;
import net.openhft.chronicle.hash.function.SerializableFunction;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.wire.TextWire;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
@Ignore
public class StatelessClientTest {
    private static final Logger LOG = LoggerFactory.getLogger(StatelessClientTest.class);

    public static final int SIZE = 2500;
    static int s_port = 9070;

    enum ToString implements SerializableFunction<Object, String> {
        INSTANCE;

        @Override
        public String apply(Object obj) {
            return obj.toString();
        }
    }

    Set<Thread> threads;

    @Before
    public void sampleThreads() {
        threads = Thread.getAllStackTraces().keySet();
    }

    @After
    public void checkThreadsShutdown() {
        checkThreadsShutdown(threads);
    }

    public static void checkThreadsShutdown(Set<Thread> threads) {
        // give them a change to stop if there were killed.
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Map<Thread, StackTraceElement[]> threadMap = Thread.getAllStackTraces();
        threadMap.keySet().removeAll(threads);
        if (!threadMap.isEmpty()) {
            System.out.println("### threads still running after the test ###");
            for (Map.Entry<Thread, StackTraceElement[]> entry : threadMap.entrySet()) {
                System.out.println(entry.getKey());
                for (StackTraceElement ste : entry.getValue()) {
                    System.out.println("\t" + ste);
                }
            }
            try {
                for (Thread thread : threadMap.keySet()) {
                    if (thread.isAlive()) {
                        System.out.println("Waiting for " + thread);
                        thread.join(1000);
                        if (thread.isAlive()) {
                            System.out.println("Forcing " + thread + " to die");
                            thread.stop();
                        }
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Ignore
    @Test
    public void testMultipleClientThreads() throws IOException, InterruptedException {

        int nThreads = 2;
        final ExecutorService executorService = Executors.newFixedThreadPool(nThreads);

        int count = 50000;
        final CountDownLatch latch = new CountDownLatch(count * 2);
        final AtomicInteger got = new AtomicInteger();

        long startTime = System.currentTimeMillis();

        ChronicleEngine chronicleEngine = new ChronicleEngine();
        chronicleEngine.setMap("test", ChronicleMapBuilder
                .of(double.class, double.class)
                .entries(50000)
                .putReturnsNull(true)
                .create());

        final RemoteMapSupplier<Integer, Integer> stringStringRemoteMapSupplier
                = new RemoteMapSupplier<>(
                Integer.class,
                Integer.class,
                chronicleEngine,
                TextWire::new);

        try (ChronicleMap<Integer, Integer> client = stringStringRemoteMapSupplier.get()) {

            for (int i = 0; i < count; i++) {
                final int j = i;
                executorService.submit(() -> {
                    try {
                        client.put(j, j);
                        latch.countDown();
                    } catch (Error | Exception e) {
                        LOG.error("", e);
                    }
                });
            }

            for (int i = 0; i < count; i++) {
                final int j = i;

                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {

                            Integer result = client.get(j);

                            if (result == null) {
                                System.out.print("entry not found so re-submitting");
                                executorService.submit(this);
                                return;
                            }

                            if (result.equals(j)) {
                                got.incrementAndGet();

                            } else {
                                System.out.println("expected j=" + j + " but got back=" +
                                        result);
                            }

                            latch.countDown();
                        } catch (Error | Exception e) {
                            e.printStackTrace();
                            LOG.error("", e);

                        }
                    }
                });
            }

            latch.await(25, TimeUnit.SECONDS);
            System.out.println("" + count + " messages took " +
                    TimeUnit.MILLISECONDS.toSeconds(System
                            .currentTimeMillis() - startTime) + " seconds, using " +
                    nThreads + " threads");

            assertEquals(count, got.get());

        } finally {
            executorService.shutdownNow();
            executorService.awaitTermination(1000, TimeUnit.SECONDS);
        }

    }

}

