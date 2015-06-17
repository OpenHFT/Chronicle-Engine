package net.openhft.chronicle.engine.fs;

import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.tree.TopologicalEvent;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import org.junit.Test;

import java.util.Map;

/**
 * Created by peter on 12/06/15.
 */
public class ConfigurationFSTest {
    @Test
    public void addMountPoints() {
        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);

        AssetTree at = new VanillaAssetTree().forTesting();
        at.registerSubscriber("", TopologicalEvent.class, System.out::println);
        at.registerSubscriber("/Data", TopologicalEvent.class, System.out::println);

        new ConfigurationFS("/etc", null).subscribeTo(at);
        Map<String, String> etc = at.acquireMap("/etc", String.class, String.class);
        etc.put(ConfigurationFS.CLUSTERS, "cluster1: {\n" +
                "  host1: {\n" +
                "     hostId: 1\n" +
                "     tcpBufferSize: 65536,\n" +
                "     hostname: localhost,\n" +
                "     port: 8088,\n" +
                "     timeoutMs: 1000,\n" +
                "  },\n" +
                "  host2: {\n" +
                "     hostId: 2\n" +
                "     tcpBufferSize: 65536,\n" +
                "     hostname: localhost,\n" +
                "     port: 8088,\n" +
                "     timeoutMs: 1000,\n" +
                "  },\n" +
                "  host3: {\n" +
                "     hostId: 3\n" +
                "     tcpBufferSize: 65536,\n" +
                "     hostname: localhost,\n" +
                "     port: 8088,\n" +
                "     timeoutMs: 1000,\n" +
                "  }\n" +
                "}\n");
        etc.put(ConfigurationFS.FSTAB, "# mount points\n" +
                "mounts: [\n" +
                "    !ChronicleMapGroupFS {\n" +
                "     spec:  $TARGET/ChMaps,\n" +
                "     name: /ChMaps,\n" +
                "     cluster: cluster1,\n" +
                "     maxEntries: 10000,\n" +
                "     averageValueSize: 10000,\n" +
                "     putReturnsNull: true,\n" +
                "     removeReturnsNull: true\n" +
                "  },\n" +
                "  !FilePerKeyGroupFS {\n" +
                "    spec: One,\n" +
                "    name: /Data/One,\n" +
                "    valueType: !type String,\n" +
                "    recurse: false\n" +
                "  },\n" +
                "  !FilePerKeyGroupFS {\n" +
                "    spec: Two,\n" +
                "    name: /Data/Two,\n" +
                "    valueType: !type String,\n" +
                "    recurse: false\n" +
                "  },\n" +
                "  !FilePerKeyGroupFS {\n" +
                "    spec: Three,\n" +
                "    name: /Data/Three,\n" +
                "    valueType: !type String,\n" +
                "    recurse: true\n" +
                "  }\n" +
                "]");
    }
}