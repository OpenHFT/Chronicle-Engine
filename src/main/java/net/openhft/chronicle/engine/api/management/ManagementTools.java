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

import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.api.management.mbean.AssetTreeJMX;
import net.openhft.chronicle.engine.map.ObjectKVSSubscription;
import net.openhft.chronicle.engine.map.ObjectKeyValueStore;
import net.openhft.chronicle.engine.tree.TopologicalEvent;
import net.openhft.lang.thread.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by peter.lawrey on 16/06/2015.
 */
public enum ManagementTools {
    ;

    private static final Logger LOGGER = LoggerFactory.getLogger(ManagementTools.class);

    //JMXConnectorServer for create jmx service
    private static JMXConnectorServer jmxServer;

    //MBeanServer for register MBeans
    private static MBeanServer mbs = null;

    //number of AssetTree enabled for management.
    private static int count = 0;

    public static int getCount(){
        return count;
    }

    private static void startJMXRemoteService() throws IOException {
        if(jmxServer==null) {
            mbs = ManagementFactory.getPlatformMBeanServer();

            // Create the RMI registry on port 9000
            java.rmi.registry.LocateRegistry.createRegistry(9000);

            // Build a URL which tells the RMIConnectorServer to bind to the RMIRegistry running on port 9000
            JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:9000/jmxrmi");
            Map<String,String> env = new HashMap<String, String>();
            env.put("com.sun.management.jmxremote", "true");
            env.put("com.sun.management.jmxremote.ssl", "false");
            env.put("com.sun.management.jmxremote.authenticate", "false");

            jmxServer = JMXConnectorServerFactory.newJMXConnectorServer(url, env, mbs);
            jmxServer.start();
        }
    }

    private static void stopJMXRemoteService() throws IOException {
        if(jmxServer!=null) {
            mbs = null;
            jmxServer.stop();
        }
    }

    /**
     * It will enable the management for given object of AssetTree type.
     * It will create a object of MBeanServer and register AssetTree.
     *
     * @param assetTree the object of AssetTree type for enable management
     * @return void
     */
    public static void enableManagement(AssetTree assetTree) {
        try{
            startJMXRemoteService();
            count++;
        }catch (IOException ie){
            LOGGER.error("Error while enable management",ie);
        }
        registerViewofTree(assetTree);
    }

    public static void disableManagement(AssetTree assetTree) {
        //TODO - unregister management service
        count--;

        try {
            if(count==0) {
                stopJMXRemoteService();
            }
        } catch (IOException e) {
            LOGGER.error("Error while disable management", e);
        }
    }

    private static void registerViewofTree(AssetTree tree) {
        ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory("tree-watcher", true));
        tree.registerSubscriber("", TopologicalEvent.class, e ->
                        // give the collection time to be setup.
                        ses.schedule(() -> handleTreeUpdate(tree, e, ses), 50, TimeUnit.MILLISECONDS)
        );
    }

    private static void handleTreeUpdate(AssetTree tree, TopologicalEvent e, ScheduledExecutorService ses) {
        try {
            String treeName = tree.toString();
            if (e.added()) {
                String assetFullName = e.fullName();
                Asset asset = tree.getAsset(assetFullName);
                if (asset == null) {
                    return;
                }
                ObjectKeyValueStore view = asset.getView(ObjectKeyValueStore.class);
                if (view == null) {
                    //todo keep this for future purpose
                    return;
                } else {

                    ObjectKVSSubscription objectKVSSubscription = asset.getView(ObjectKVSSubscription.class);

                    ObjectName atName = new ObjectName(createObjectNameUri(e.assetName(),e.name(),treeName));

                    tree.registerSubscriber(e.fullName(), MapEvent.class, (MapEvent me) ->
                            ses.schedule(() -> handleAssetUpdate(view,atName,objectKVSSubscription), 100, TimeUnit.MILLISECONDS));

                    AssetTreeJMX atBean = new AssetTreeJMX(view,objectKVSSubscription,e.assetName() + "-" + e.name());

                    registerTreeWithMBean(atBean, atName);
                }
            } else {
                ObjectName atName = new ObjectName(createObjectNameUri(e.assetName(),e.name(),treeName));
                unregisterTreeWithMBean(atName);
            }
        } catch (Throwable t) {
            LOGGER.error("Error while handle AssetTree update", t);
        }
    }

    private static void handleAssetUpdate(ObjectKeyValueStore view, ObjectName atName, ObjectKVSSubscription objectKVSSubscription){
        try {
            if(mbs.isRegistered(atName)){

                AttributeList list = new AttributeList();
                list.add(new Attribute("Size",view.longSize()));
                list.add(new Attribute("KeyType",view.keyType().getName()));
                list.add(new Attribute("ValueType",view.valueType().getName()));
                list.add(new Attribute("TopicSubscriberCount",objectKVSSubscription.topicSubscriberCount()));
                list.add(new Attribute("EntrySubscriberCount",objectKVSSubscription.entrySubscriberCount()));
                list.add(new Attribute("KeySubscriberCount",objectKVSSubscription.keySubscriberCount()));

                mbs.setAttributes(atName,list);

            }
        } catch (Throwable t) {
            LOGGER.error("Error while handle Asset update", t);
        }
    }

    private static String createObjectNameUri(String assetName, String eventName, String treeName){
        System.out.println(treeName);
        StringBuilder sb = new StringBuilder(256);
        sb.append("net.openhft.chronicle.engine.tree");
        sb.append(":type=");
        sb.append(treeName.substring(treeName.lastIndexOf(".")+1));
        //sb.append("net.openhft.chronicle.engine.api.tree:type=AssetTree");

        String[] names = assetName.split("/");
        for (int i = 1; i < names.length; i++) {
            sb.append(",side").append(i).append("=").append(names[i]);
        }
        sb.append(",name=").append(eventName);

        return sb.toString();
    }

    private static void registerTreeWithMBean(AssetTreeJMX atBean,ObjectName atName){
        try {
            mbs.registerMBean(atBean, atName);
        } catch (InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
            LOGGER.error("Error reigster AssetTree with MBean", e);
        }
    }

    private static void unregisterTreeWithMBean(ObjectName atName){
        try {
            mbs.unregisterMBean(atName);
        } catch (InstanceNotFoundException | MBeanRegistrationException e) {
            LOGGER.error("Error unreigster AssetTree with MBean",e);
        }
    }
}
