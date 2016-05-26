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

package net.openhft.chronicle.engine.fs;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.engine.VanillaAssetTreeEgMain;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.tree.TopologicalEvent;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by peter on 12/06/15.
 */
public class ConfigurationFSTest {

    private static AtomicReference<Throwable> t = new AtomicReference();

    private ThreadDump threadDump;

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    @After
    public void checkThreadDump() {
        threadDump.ignore("all-trees-watcher");
        threadDump.assertNoNewThreads();
    }

    @After
    public void afterMethod() {
        final Throwable th = t.getAndSet(null);
        if (th != null) throw Jvm.rethrow(th);
    }



    @Test
    public void addMountPoints() {
        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);

        AssetTree at = new VanillaAssetTree().forTesting(x -> t.compareAndSet(null, x));
        at.registerSubscriber("", TopologicalEvent.class, System.out::println);
        at.registerSubscriber("/Data", TopologicalEvent.class, System.out::println);

        new ConfigurationFS("/etc", null, OS.TARGET + "/confstest").subscribeTo(at);
        Map<String, String> etc = at.acquireMap("/etc", String.class, String.class);
        etc.put(ConfigurationFS.CLUSTERS, "cluster1: {\n" +
                "  context:  !EngineClusterContext  { }\n" +
                "  host1: {\n" +
                "     hostId: 1\n" +
                "     tcpBufferSize: 65536,\n" +
                "     connectUri: localhost:8188,\n" +
                "     timeoutMs: 1000,\n" +
                "  },\n" +
                "  host2: {\n" +
                "     hostId: 2\n" +
                "     tcpBufferSize: 65536,\n" +
                "     connectUri: localhost:8288,\n" +
                "     timeoutMs: 1000,\n" +
                "  },\n" +
                "  host3: {\n" +
                "     hostId: 3\n" +
                "     tcpBufferSize: 65536,\n" +
                "     connectUri: localhost:8388,\n" +
                "     timeoutMs: 1000,\n" +
                "  }\n" +
                "}\n");
        etc.put(ConfigurationFS.FSTAB, "# mount points\n" +
                "ChronMaps: !ChronicleMapGroupFS {\n" +
                "     spec:  $TARGET/ChMaps,\n" +
                "     name: /ChMaps,\n" +
                "     cluster: cluster1,\n" +
                "     maxEntries: 10000,\n" +
                "     averageValueSize: 10000,\n" +
                "     putReturnsNull: true,\n" +
                "     removeReturnsNull: true\n" +
                "  }\n" +
                "One: !FilePerKeyGroupFS {\n" +
                "    spec: One,\n" +
                "    name: /Data/One,\n" +
                "    valueType: !type String,\n" +
                "    recurse: false\n" +
                "  }\n" +
                "Two: !FilePerKeyGroupFS {\n" +
                "    spec: Two,\n" +
                "    name: /Data/Two,\n" +
                "    valueType: !type String,\n" +
                "    recurse: false\n" +
                "  }\n" +
                "Three: !FilePerKeyGroupFS {\n" +
                "    spec: Three,\n" +
                "    name: /Data/Three,\n" +
                "    valueType: !type String,\n" +
                "    recurse: true\n" +
                "  }\n");

        VanillaAssetTreeEgMain.registerTextViewofTree("ConfigFS", at);
        Jvm.pause(100);
        at.close();
    }
}