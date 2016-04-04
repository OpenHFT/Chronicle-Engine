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

import com.google.common.collect.testing.MapTestSuiteBuilder;
import com.google.common.collect.testing.SampleElements;
import com.google.common.collect.testing.TestMapGenerator;
import com.google.common.collect.testing.features.CollectionFeature;
import com.google.common.collect.testing.features.CollectionSize;
import junit.framework.Test;
import junit.framework.TestSuite;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.map.MapClientTest.LocalMapSupplier;
import net.openhft.chronicle.engine.map.MapClientTest.RemoteMapSupplier;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.collect.testing.features.MapFeature.*;

@SuppressWarnings("all")

@RunWith(AllTests.class)
public class GuavaEngineTest {
    public static final WireType WIRE_TYPE = WireType.TEXT;

    @NotNull
    public static Test suite() throws IOException {

        MapTestSuiteBuilder using = MapTestSuiteBuilder.using(new RemoteTestGenerator(new
                VanillaAssetTree().forTesting(t -> t.printStackTrace())));

        TestSuite remoteMapTests = using.named("Chronicle RemoteEngine Guava tests")
                .withFeatures(GENERAL_PURPOSE)
                .withFeatures(CollectionSize.ANY)
                .withFeatures(CollectionFeature.REMOVE_OPERATIONS)
                .withFeatures(RESTRICTS_KEYS, RESTRICTS_VALUES)
                .withTearDown(() -> {
                    try {
                        ((RemoteTestGenerator) using.getSubjectGenerator()).close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }).createTestSuite();

        TestSuite localMapTests = MapTestSuiteBuilder.using(new LocalTestGenerator())
                .named("Chronicle LocalEngine Guava tests")
                .withFeatures(GENERAL_PURPOSE)
                .withFeatures(CollectionSize.ANY)
                .withFeatures(CollectionFeature.REMOVE_OPERATIONS)
                .withFeatures(RESTRICTS_KEYS, RESTRICTS_VALUES)
                .createTestSuite();

        TestSuite tests = new TestSuite();
        //  tests.addTest(remoteMapTests);
        tests.addTest(localMapTests);
        return tests;
    }

    @NotNull
    static ConcurrentMap<CharSequence, CharSequence> newStrStrRemoteMap(@NotNull AssetTree assetTree) {

        try {
            return new RemoteMapSupplier<>("guava.test.host.port", CharSequence.class, CharSequence.class,
                    WireType.TEXT, assetTree, "test").get();
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
    }

    @NotNull
    static ConcurrentMap<CharSequence, CharSequence> newStrStrLocalMap() {
        return new LocalMapSupplier(CharSequence.class, CharSequence.class, new
                VanillaAssetTree().forTesting(t -> t.printStackTrace())).get();
    }

    @AfterClass
    public void testTearDown() {
        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }

    @After
    public void tearDown() {
        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }

    static abstract class TestGenerator
            implements TestMapGenerator<CharSequence, CharSequence> {

        @Nullable
        abstract Map<CharSequence, CharSequence> newMap();

        @NotNull
        public CharSequence[] createKeyArray(int length) {
            return new CharSequence[length];
        }

        @NotNull
        @Override
        public CharSequence[] createValueArray(int length) {
            return new CharSequence[length];
        }

        @NotNull
        @Override
        public SampleElements<Map.Entry<CharSequence, CharSequence>> samples() {
            return SampleElements.mapEntries(
                    new SampleElements<CharSequence>(
                            "key1", "key2", "key3", "key4", "key5"
                    ),
                    new SampleElements<CharSequence>(
                            "val1", "val2", "val3", "val4", "val5"
                    )
            );
        }

        @Nullable
        @Override
        public Map<CharSequence, CharSequence> create(@NotNull Object... objects) {
            Map<CharSequence, CharSequence> map = newMap();
            map.clear();
            for (Object obj : objects) {
                Map.Entry e = (Map.Entry) obj;
                map.put((CharSequence) e.getKey(),
                        (CharSequence) e.getValue());
            }
            return map;
        }

        @NotNull
        @Override
        public Map.Entry<CharSequence, CharSequence>[] createArray(int length) {
            return new Map.Entry[length];
        }

        @Override
        public Iterable<Map.Entry<CharSequence, CharSequence>> order(
                List<Map.Entry<CharSequence, CharSequence>> insertionOrder) {
            return insertionOrder;
        }
    }

    static class CHMTestGenerator extends TestGenerator {

        @Nullable
        @Override
        Map<CharSequence, CharSequence> newMap() {
            return null;
        }
    }

    static class RemoteTestGenerator extends CHMTestGenerator implements Closeable {
        @NotNull
        private final AssetTree remoteAssetTree;
        @NotNull
        private final AssetTree assetTree;

        public RemoteTestGenerator(@NotNull AssetTree assetTree) throws IOException {
            this.assetTree = assetTree;
            TCPRegistry.createServerSocketChannelFor("guava.test.host.port");
            final ServerEndpoint serverEndpoint = new ServerEndpoint("guava.test.host.port", assetTree);

            final String hostname = "localhost";
            this.remoteAssetTree = new VanillaAssetTree().forRemoteAccess("guava.test" +
                    ".host.port", WIRE_TYPE, t -> t.printStackTrace());
        }

        @NotNull
        @Override
        Map<CharSequence, CharSequence> newMap() {
            return newStrStrRemoteMap(remoteAssetTree);
        }

        @Override
        public void close() throws IOException {
            assetTree.close();
            remoteAssetTree.close();
            TcpChannelHub.closeAllHubs();
            TCPRegistry.reset();
        }
    }

    static class LocalTestGenerator extends CHMTestGenerator {
        @NotNull
        @Override
        Map<CharSequence, CharSequence> newMap() {
            return newStrStrLocalMap();
        }
    }
}