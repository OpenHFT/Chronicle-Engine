/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.engine.map;

import com.google.common.collect.testing.MapTestSuiteBuilder;
import com.google.common.collect.testing.SampleElements;
import com.google.common.collect.testing.TestMapGenerator;
import com.google.common.collect.testing.features.CollectionFeature;
import com.google.common.collect.testing.features.CollectionSize;
import junit.framework.Test;
import junit.framework.TestSuite;
import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.map.MapClientTest.LocalMapSupplier;
import net.openhft.chronicle.engine.map.MapClientTest.RemoteMapSupplier;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.TextWire;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.collect.testing.features.MapFeature.*;

@SuppressWarnings("all")

@RunWith(AllTests.class)
public class GuavaEngineTest   {

    @NotNull
    public static Test suite() {
        AssetTree assetTree = new VanillaAssetTree().forTesting();
/*
        TestSuite remoteMapTests = MapTestSuiteBuilder.using(new RemoteTestGenerator(assetTree))
                .named("Chronicle RemoteEngine Guava tests")
                .withFeatures(GENERAL_PURPOSE)
                .withFeatures(CollectionSize.ANY)
                .withFeatures(CollectionFeature.REMOVE_OPERATIONS)
                .withFeatures(RESTRICTS_KEYS, RESTRICTS_VALUES)
                .createTestSuite();
*/

        TestSuite localMapTests = MapTestSuiteBuilder.using(new LocalTestGenerator())
                .named("Chronicle LocalEngine Guava tests")
                .withFeatures(GENERAL_PURPOSE)
                .withFeatures(CollectionSize.ANY)
                .withFeatures(CollectionFeature.REMOVE_OPERATIONS)
                .withFeatures(RESTRICTS_KEYS, RESTRICTS_VALUES)
                .createTestSuite();

        TestSuite tests = new TestSuite();
   //     tests.addTest(remoteMapTests);
        // tests.addTest(localMapTests);
        return tests;
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

    static class RemoteTestGenerator extends CHMTestGenerator {
        private final AssetTree assetTree;

        public RemoteTestGenerator(AssetTree assetTree) {
            this.assetTree = assetTree;
        }

        @NotNull
        @Override
        Map<CharSequence, CharSequence> newMap() {
            return newStrStrRemoteMap(assetTree);
        }
    }

    static class LocalTestGenerator extends CHMTestGenerator {
        @NotNull
        @Override
        Map<CharSequence, CharSequence> newMap() {
            return newStrStrLocalMap();
        }
    }

    @NotNull
    static ConcurrentMap<CharSequence, CharSequence> newStrStrRemoteMap(AssetTree assetTree) {

        try {
            return new RemoteMapSupplier<>(CharSequence.class, CharSequence.class,
                    TextWire::new, assetTree).get();
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
    }

    @NotNull
    static ConcurrentMap<CharSequence, CharSequence> newStrStrLocalMap() {

        try {
            return new LocalMapSupplier(CharSequence.class, CharSequence.class, new VanillaAssetTree().forTesting()).get();
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
    }
}