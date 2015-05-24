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
import junit.framework.TestCase;
import junit.framework.TestSuite;
import net.openhft.chronicle.engine.client.internal.ChronicleEngine;
import net.openhft.chronicle.engine.map.MapClientTest.LocalMapSupplier;
import net.openhft.chronicle.engine.map.MapClientTest.RemoteMapSupplier;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.wire.TextWire;
import org.junit.Assert;
import org.junit.Ignore;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.testing.features.MapFeature.*;


@SuppressWarnings("all")
public class GuavaEngineTest extends TestCase {

    public static Test suite() {
        TestSuite remoteMapTests = MapTestSuiteBuilder.using(new RemoteTestGenerator())
                .named("Chronicle RemoteEngine Guava tests")
                .withFeatures(GENERAL_PURPOSE)
                .withFeatures(CollectionSize.ANY)
                .withFeatures(CollectionFeature.REMOVE_OPERATIONS)
                .withFeatures(RESTRICTS_KEYS, RESTRICTS_VALUES)
                .createTestSuite();

        TestSuite localMapTests = MapTestSuiteBuilder.using(new LocalTestGenerator())
                .named("Chronicle LocalEngine Guava tests")
                .withFeatures(GENERAL_PURPOSE)
                .withFeatures(CollectionSize.ANY)
                .withFeatures(CollectionFeature.REMOVE_OPERATIONS)
                .withFeatures(RESTRICTS_KEYS, RESTRICTS_VALUES)
                .createTestSuite();

        TestSuite tests = new TestSuite();
     //   tests.addTest(remoteMapTests);
     //   tests.addTest(localMapTests);
        return tests;
    }

    static abstract class TestGenerator
            implements TestMapGenerator<CharSequence, CharSequence> {

        abstract Map<CharSequence, CharSequence> newMap();

        public CharSequence[] createKeyArray(int length) {
            return new CharSequence[length];
        }

        @Override
        public CharSequence[] createValueArray(int length) {
            return new CharSequence[length];
        }

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

        @Override
        public Map<CharSequence, CharSequence> create(Object... objects) {
            Map<CharSequence, CharSequence> map = newMap();
            for (Object obj : objects) {
                Map.Entry e = (Map.Entry) obj;
                map.put((CharSequence) e.getKey(),
                        (CharSequence) e.getValue());
            }
            return map;
        }

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

        @Override
        Map<CharSequence, CharSequence> newMap() {
            return null;
        }
    }

    static class RemoteTestGenerator extends CHMTestGenerator {
        @Override
        Map<CharSequence, CharSequence> newMap() {
            return newStrStrRemoteMap();
        }
    }

    static class LocalTestGenerator extends CHMTestGenerator {
        @Override
        Map<CharSequence, CharSequence> newMap() {
            return newStrStrLocalMap();
        }
    }

    static ChronicleMap<CharSequence, CharSequence> newStrStrRemoteMap() {

        RemoteMapSupplier remoteMapSupplierTemp = null;
        try {
            remoteMapSupplierTemp = new RemoteMapSupplier(CharSequence.class, CharSequence.class,
                    new ChronicleEngine(), TextWire.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        final RemoteMapSupplier remoteMapSupplier = remoteMapSupplierTemp;
        final ChronicleMap spy = Mockito.spy(remoteMapSupplier.get());

        Mockito.doAnswer(invocationOnMock -> {
            remoteMapSupplier.close();
            return null;
        }).when(spy).close();

        return spy;
    }

    static ChronicleMap<CharSequence, CharSequence> newStrStrLocalMap() {

        LocalMapSupplier localMapSupplierTemp = null;
        try {
            localMapSupplierTemp = new LocalMapSupplier(CharSequence.class, CharSequence.class);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
        final LocalMapSupplier localMapSupplier = localMapSupplierTemp;
        final ChronicleMap spy = Mockito.spy(localMapSupplier.get());

        Mockito.doAnswer(invocationOnMock -> {
            localMapSupplier.close();
            return null;
        }).when(spy).close();
        Mockito.when(spy.toString()).thenCallRealMethod();
        return spy;
    }

}