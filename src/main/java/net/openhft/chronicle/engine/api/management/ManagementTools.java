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

package net.openhft.chronicle.engine.api.management;

import net.openhft.chronicle.engine.api.management.mbean.AssetTreeDynamicMBean;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.SubscriptionKeyValueStore;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.map.ObjectKeyValueStore;
import net.openhft.chronicle.engine.map.ObjectSubscription;
import net.openhft.chronicle.engine.tree.HostIdentifier;
import net.openhft.chronicle.engine.tree.TopologicalEvent;
import net.openhft.chronicle.network.api.session.SessionDetails;
import net.openhft.chronicle.network.api.session.SessionProvider;
import net.openhft.chronicle.threads.Threads;
import net.openhft.lang.thread.NamedThreadFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.rmi.registry.LocateRegistry;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by peter.lawrey on 16/06/2015.
 */
public enum ManagementTools {
    ;

    private static final Logger LOG = LoggerFactory.getLogger(ManagementTools.class);

    //JMXConnectorServer for create jmx service
    private static JMXConnectorServer jmxServer;

    //MBeanServer for register MBeans
    @Nullable
    private static MBeanServer mbs = null;

    private static AssetTreeDynamicMBean dynamicMBean;

    //number of AssetTree enabled for management.
    private static int count = 0;

    public static int getCount() {
        return count;
    }

    private static void startJMXRemoteService() throws IOException {
        if (jmxServer == null) {
            mbs = ManagementFactory.getPlatformMBeanServer();

            // Create the RMI registry on port 9000
            LocateRegistry.createRegistry(9000);

            // Build a URL which tells the RMIConnectorServer to bind to the RMIRegistry running on port 9000
            JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:9000/jmxrmi");
            Map<String, String> env = new HashMap<>();
            env.put("com.sun.management.jmxremote", "true");
            env.put("com.sun.management.jmxremote.ssl", "false");
            env.put("com.sun.management.jmxremote.authenticate", "false");

            jmxServer = JMXConnectorServerFactory.newJMXConnectorServer(url, env, mbs);
            jmxServer.start();
        }
    }

    private static void stopJMXRemoteService() throws IOException {
        if (jmxServer != null) {
            mbs = null;
            jmxServer.stop();
        }
    }

    /**
     * It will enable the management for given object of AssetTree type. It will create a object of
     * MBeanServer and register AssetTree.
     *
     * @param assetTree the object of AssetTree type for enable management
     */
    public static void enableManagement(@NotNull AssetTree assetTree) {
        try {
            startJMXRemoteService();
            count++;
        } catch (IOException ie) {
            LOG.warn("Error while enable management", ie);
        }
        registerViewofTree(assetTree);
    }

    public static void enableManagement(@NotNull AssetTree assetTree, int port) {
        try {
            startJMXRemoteService();

            count++;
        } catch (IOException ie) {
            LOG.warn("Error while enable management", ie);
        }
        registerViewofTree(assetTree);
    }

    public static void disableManagement(@NotNull AssetTree assetTree) {

        String treeName = assetTree.toString();
        try {
            Set<ObjectName> objNames = mbs.queryNames(new ObjectName("*:type=" + treeName + ",*"), null);
            objNames.forEach((atName) -> ManagementTools.unregisterTreeWithMBean(atName));
        } catch (MalformedObjectNameException e) {
            LOG.warn("Error while disable management", e);
        }
        count--;

        try {
            if (count == 0) {
                stopJMXRemoteService();
            }
        } catch (IOException e) {
            LOG.warn("Error while stopping JMX remote service", e);
        }
    }

    private static void registerViewofTree(@NotNull AssetTree tree) {
        Threads.withThreadGroup(tree.root().getView(ThreadGroup.class), () -> {
            ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor(
                    new NamedThreadFactory("tree-watcher", true));

            SessionProvider view = tree.root().findView(SessionProvider.class);
            final SessionDetails sessionDetails = view.get();

            ses.submit(() -> {
                // set the session details on the JMX thread, to the same as the server system session details.
                final SessionProvider view0 = tree.root().findView(SessionProvider.class);
                view0.set(sessionDetails);
            });

            tree.registerSubscriber("", TopologicalEvent.class, e -> {
                        // give the collection time to be setup.
                if (e.assetName() != null) {
                            ses.schedule(() -> handleTreeUpdate(tree, e, ses), 2000, TimeUnit.MILLISECONDS);
                        }
                    }
            );
            return null;
        });
    }

    private static void handleTreeUpdate(@NotNull AssetTree tree, @NotNull TopologicalEvent e, @NotNull ScheduledExecutorService ses) {
        try {
            HostIdentifier hostIdentifier = tree.root().getView(HostIdentifier.class);
            int hostId = hostIdentifier == null ? 0 : hostIdentifier.hostId();
            String treeName = tree.toString();
            if (e.added()) {
                String assetFullName = e.fullName();
                Asset asset = tree.getAsset(assetFullName);
                if (asset == null) {
                    return;
                }

                ObjectKeyValueStore<Object, Object> view = null;

                for (Class c : new Class[]{ObjectKeyValueStore.class, SubscriptionKeyValueStore.class}) {

                    view = (ObjectKeyValueStore) asset.getView(c);
                    if (view != null) {
                        break;
                    }
                }

                if (view == null) {
                    return;
                }

                final ObjectKeyValueStore view0 = view;
                ObjectSubscription objectSubscription = asset.getView(ObjectSubscription.class);
                //ObjectName atName = new ObjectName(createObjectNameUri(e.assetName(),e.name(),treeName));

                //start Dynamic MBeans Code
                Map<String, String> m = new HashMap<>();
                m.put("size", "" + view.longSize());
                m.put("keyType", view.keyType().getName());
                m.put("valueType", view.valueType().getName());
                m.put("topicSubscriberCount", "" + objectSubscription.topicSubscriberCount());
                m.put("keySubscriberCount", "" + objectSubscription.keySubscriberCount());
                m.put("entrySubscriberCount", "" + objectSubscription.entrySubscriberCount());
                m.put("keyStoreValue", objectSubscription.getClass().getName());
                m.put("path", e.assetName() + "-" + e.name());

                for (int i = 0; i < view.segments(); i++) {
                    view.entriesFor(i, entry -> {
                        if (entry.getValue().toString().length() > 256) {
                            m.put("~" + entry.getKey().toString(), entry.getValue().toString().substring(0, 256) + "...");
                        } else {
                            m.put("~" + entry.getKey().toString(), entry.getValue().toString());
                        }
                    });
                }
                dynamicMBean = new AssetTreeDynamicMBean(m);
                ObjectName atName = new ObjectName(createObjectNameUri(hostId, e.assetName(), e.name(), treeName));
                registerTreeWithMBean(dynamicMBean, atName);
                //end Dynamic MBeans Code

                tree.registerSubscriber(e.fullName(), MapEvent.class, (MapEvent me) ->
                        ses.schedule(() -> handleAssetUpdate(view0, atName, objectSubscription, e.assetName() + "-" + e.name()), 100, TimeUnit.MILLISECONDS));

                //AssetTreeJMX atBean = new AssetTreeJMX(view,objectKVSSubscription,e.assetName() + "-" + e.name(),getMapAsString(view));
                //registerTreeWithMBean(atBean, atName);

            } else {
                ObjectName atName = new ObjectName(createObjectNameUri(hostId, e.assetName(), e.name(), treeName));
                unregisterTreeWithMBean(atName);
            }
        } catch (Throwable t) {
            LOG.warn("Error while handle AssetTree update", t);
        }
    }

    private static void handleAssetUpdate(@NotNull ObjectKeyValueStore view, ObjectName atName, @NotNull ObjectSubscription objectSubscription, String path) {
        try {
            if (mbs != null && mbs.isRegistered(atName)) {
                /*AttributeList list = new AttributeList();
                list.add(new Attribute("Size",view.longSize()));
                list.add(new Attribute("Entries",getMapAsString(view)));
                list.add(new Attribute("KeyType",view.keyType().getName()));
                list.add(new Attribute("ValueType",view.valueType().getName()));
                list.add(new Attribute("TopicSubscriberCount",objectKVSSubscription.topicSubscriberCount()));
                list.add(new Attribute("EntrySubscriberCount",objectKVSSubscription.entrySubscriberCount()));
                list.add(new Attribute("KeySubscriberCount",objectKVSSubscription.keySubscriberCount()));
                list.add(new Attribute("Dynamic Attribute Key","Dynamic Attribute Value"));
                mbs.setAttributes(atName,list);*/
                //start Dynamic MBeans Code
                Map m = new HashMap();
                m.put("size", "" + view.longSize());
                m.put("keyType", view.keyType().getName());
                m.put("valueType", view.valueType().getName());
                m.put("topicSubscriberCount", "" + objectSubscription.topicSubscriberCount());
                m.put("keySubscriberCount", "" + objectSubscription.keySubscriberCount());
                m.put("entrySubscriberCount", "" + objectSubscription.entrySubscriberCount());
                m.put("keyStoreValue", objectSubscription.getClass().getName());
                m.put("path", path);

                Iterator<Map.Entry> it = view.entrySetIterator();
                for (int i = 0; i < view.longSize(); i++) {
                    Map.Entry entry = it.next();
                    if (entry.getValue().toString().length() > 128) {
                        m.put("~" + entry.getKey().toString(), entry.getValue().toString().substring(0, 128) + "...");
                    } else {
                        m.put("~" + entry.getKey().toString(), entry.getValue().toString());
                    }
                }
                dynamicMBean = new AssetTreeDynamicMBean(m);
                unregisterTreeWithMBean(atName);
                registerTreeWithMBean(dynamicMBean, atName);
                //end Dynamic MBeans Code
            }
        } catch (Throwable t) {
            LOG.warn("Error while handle Asset update", t);
        }
    }

    private static String createObjectNameUri(int hostId, @NotNull String assetName, String eventName, @NotNull String treeName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("treeName=" + treeName);
        }
        StringBuilder sb = new StringBuilder(256);
        sb.append("net.openhft.chronicle.engine:tree=");
        sb.append(hostId);
        sb.append(",type=");
        sb.append(treeName);
        //sb.append("net.openhft.chronicle.engine.api.tree:type=AssetTree");

        String[] names = assetName.split("/");
        for (int i = 1; i < names.length; i++) {
            sb.append(",side").append(i).append("=").append(names[i]);
        }
        sb.append(",name=").append(eventName);
        return sb.toString();
    }

    private static void registerTreeWithMBean(AssetTreeDynamicMBean atBean, ObjectName atName) {
        try {
            if (mbs != null && !mbs.isRegistered(atName)) {
                mbs.registerMBean(atBean, atName);
            }
        } catch (@NotNull InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
            LOG.warn("Error register AssetTree with MBean", e);
        }
    }

    private static void unregisterTreeWithMBean(ObjectName atName) {
        try {
            if (mbs != null && mbs.isRegistered(atName)) {
                mbs.unregisterMBean(atName);
            }
        } catch (@NotNull InstanceNotFoundException | MBeanRegistrationException e) {
            LOG.warn("Error unregister AssetTree with MBean", e);
        }
    }

    private static String getMapAsString(@NotNull ObjectKeyValueStore view) {

        long max = view.longSize() - 1;
        if (max == -1) {
            return "{}";
        }

        StringBuilder sb = new StringBuilder();

        Iterator<Map.Entry> it = view.entrySetIterator();
        sb.append('{');
        for (int i = 0; ; i++) {
            Map.Entry e = it.next();

            String key = e.getKey().toString();
            String value = e.getValue().toString();

            sb.append(key);
            sb.append('=');

            if (value.length() > 128) {
                sb.append(value.substring(0, 128)).append("...");
            } else {
                sb.append(value);
            }

            if (i == max) {
                return sb.append('}').toString();
            }
            sb.append(", ");
        }
    }
}
