package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.onoes.Slf4jExceptionHandler;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.pool.ClassLookup;
import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.cfg.*;
import net.openhft.chronicle.engine.fs.*;
import net.openhft.chronicle.engine.map.CMap2EngineReplicator;
import net.openhft.chronicle.engine.map.ChronicleMapKeyValueStore;
import net.openhft.chronicle.engine.map.VanillaMapView;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.TopologicalEvent;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.NetworkStats;
import net.openhft.chronicle.network.NetworkStatsListener;
import net.openhft.chronicle.network.cluster.HostDetails;
import net.openhft.chronicle.wire.TextWire;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.openhft.chronicle.core.Jvm.setExceptionsHandlers;
import static net.openhft.chronicle.core.onoes.PrintExceptionHandler.WARN;

/**
 * @author Rob Austin.
 */
public class EngineInstance {

    static {

        try {
            setExceptionsHandlers(WARN, WARN, Slf4jExceptionHandler.DEBUG);
            ClassLookup classAliases = RequestContext.CLASS_ALIASES;
            ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class,
                    FilePerKeyGroupFS.class,
                    EngineCfg.class,
                    JmxCfg.class,
                    ServerCfg.class,
                    ClustersCfg.class,
                    InMemoryMapCfg.class,
                    FilePerKeyMapCfg.class,
                    ChronicleMapCfg.class,
                    MonitorCfg.class);

        } catch (Exception e) {
            System.exit(-1);
        }
    }

    static final Logger LOGGER = LoggerFactory.getLogger(EngineInstance.class);

    public static VanillaAssetTree engineMain(final int hostId, final String name) {
        try {

            TextWire yaml = TextWire.fromFile(name);

            EngineCfg installable = (EngineCfg) yaml.readObject();

            final VanillaAssetTree tree = new VanillaAssetTree(hostId).forServer(false);

            final Asset connectivityMap = tree.acquireAsset("/proc/connections/cluster/connectivity");
            connectivityMap.addWrappingRule(MapView.class, "map directly to KeyValueStore",
                    VanillaMapView::new,
                    KeyValueStore.class);
            connectivityMap.addLeafRule(EngineReplication.class, "Engine replication holder",
                    CMap2EngineReplicator::new);
            connectivityMap.addLeafRule(KeyValueStore.class, "KVS is Chronicle Map", (context, asset) ->
                    new ChronicleMapKeyValueStore(context.cluster(firstClusterName(tree)), asset));

            try {
                installable.install("/", tree);
                LOGGER.info("Engine started");
            } catch (Exception e) {
                LOGGER.error("Error starting a component, stopping", e);
                tree.close();
            }

            final Clusters clusters = tree.root().getView(Clusters.class);

            if (clusters == null || clusters.size() == 0) {
                Jvm.warn().on(EngineInstance.class, "cluster not found");
                return null;
            }
            if (clusters.size() != 1) {
                Jvm.warn().on(EngineInstance.class, "unambiguous cluster, you have " + clusters.size() + "" +
                        " clusters which one do you want to use?");
                return null;
            }

            final EngineCluster engineCluster = clusters.firstCluster();
            final HostDetails hostDetails = engineCluster.findHostDetails(hostId);
            final String connectUri = hostDetails.connectUri();
            engineCluster.clusterContext().assetRoot(tree.root());

            final NetworkStatsListener networkStatsListener = engineCluster.clusterContext()
                    .networkStatsListenerFactory()
                    .apply(engineCluster.clusterContext());

            final ServerEndpoint serverEndpoint = new ServerEndpoint(connectUri, tree, networkStatsListener);

            // we add this as close will get called when the asset tree is closed
            tree.root().addView(ServerEndpoint.class, serverEndpoint);
            tree.registerSubscriber("", TopologicalEvent.class, e -> LOGGER.info("Tree change " + e));

            // the reason that we have to do this is to ensure that the network stats are
            // replicated between all hosts, if you don't acquire a queue it wont exist and so
            // will not act as a slave in replication
            for (EngineHostDetails engineHostDetails : engineCluster.hostDetails()) {

                final int id = engineHostDetails
                        .hostId();

                tree.acquireQueue("/proc/connections/cluster/throughput/" + id,
                        String.class,
                        NetworkStats.class, engineCluster.clusterName());
            }

            return tree;
        } catch (Exception e) {
            e.printStackTrace();
            throw Jvm.rethrow(e);
        }

    }

    /**
     * @return the first cluster name
     */
    static String firstClusterName(VanillaAssetTree tree) {
        final Clusters clusters = tree.root().getView(Clusters.class);
        if (clusters == null)
            return "";
        final EngineCluster engineCluster = clusters.firstCluster();
        if (engineCluster == null)
            return "";
        return engineCluster.clusterName();
    }


}
