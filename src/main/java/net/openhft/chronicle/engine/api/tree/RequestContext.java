/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.engine.api.tree;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.pool.ClassLookup;
import net.openhft.chronicle.engine.HeartbeatHandler;
import net.openhft.chronicle.engine.api.collection.ValuesCollection;
import net.openhft.chronicle.engine.api.column.VaadinChart;
import net.openhft.chronicle.engine.api.column.ColumnView;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.*;
import net.openhft.chronicle.engine.api.session.Heartbeat;
import net.openhft.chronicle.engine.api.set.EntrySetView;
import net.openhft.chronicle.engine.api.set.KeySetView;
import net.openhft.chronicle.engine.cfg.*;
import net.openhft.chronicle.engine.fs.ChronicleMapGroupFS;
import net.openhft.chronicle.engine.fs.EngineConnectionManager;
import net.openhft.chronicle.engine.map.AuthenticatedKeyValueStore;
import net.openhft.chronicle.engine.map.ObjectKeyValueStore;
import net.openhft.chronicle.engine.map.ObjectSubscription;
import net.openhft.chronicle.engine.map.RawKVSSubscription;
import net.openhft.chronicle.engine.query.Filter;
import net.openhft.chronicle.engine.query.Operation.OperationType;
import net.openhft.chronicle.engine.server.internal.EngineNetworkStatsListener;
import net.openhft.chronicle.engine.server.internal.EngineWireNetworkContext;
import net.openhft.chronicle.engine.server.internal.MapReplicationHandler;
import net.openhft.chronicle.engine.server.internal.UberHandler;
import net.openhft.chronicle.engine.tree.QueueView;
import net.openhft.chronicle.engine.tree.TopologicalEvent;
import net.openhft.chronicle.engine.tree.TopologySubscription;
import net.openhft.chronicle.network.TcpEventHandler;
import net.openhft.chronicle.network.cluster.ClusterContext;
import net.openhft.chronicle.network.cluster.HostIdConnectionStrategy;
import net.openhft.chronicle.wire.QueryWire;
import net.openhft.chronicle.wire.VanillaWireParser;
import net.openhft.chronicle.wire.WireParser;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Proxy;

/**
 * Created by peter on 24/05/15.
 */
public class RequestContext implements Cloneable {
    public static final ClassLookup CLASS_ALIASES = ClassLookup.create();

    static {

        loadDefaultAliases();
        addAliasLocal(VaadinChart.class, "BarChart");
        addAliasLocal(ColumnView.class, "COLUMN");
        addAliasLocal(QueueView.class, "Queue");
        addAliasLocal(MapView.class, "Map");
        addAlias(MapEvent.class, "MapEvent");
        addAlias(TopologicalEvent.class, "TopologicalEvent");
        addAliasLocal(EntrySetView.class, "EntrySet");
        addAliasLocal(KeySetView.class, "KeySet");
        addAliasLocal(ValuesCollection.class, "Values");
        addAlias(Replication.class, "Replication");
        addAlias(Publisher.class, "Publisher, Pub");
        addAlias(TopicPublisher.class, "TopicPublisher, TopicPub");
        addAlias(ObjectSubscription.class, "Subscription");
        addAlias(TopologySubscription.class, "TopologySubscription");
        addAlias(Reference.class, "Reference, Ref");
        addAlias(Heartbeat.class, "Heartbeat");
        addAlias(Filter.class, "Filter");
        addAlias(net.openhft.chronicle.engine.query.Operation.class, "Operation");
        // TODO replace with a proper implementation.
        addAlias(Proxy.class, "set-proxy");
        addAlias(Operation.class, "QueryOperation");
        addAlias(OperationType.class, "QueryOperationType");
        addAlias(UberHandler.Factory.class, "UberHandlerFactory");
        addAlias(VanillaWireOutPublisherFactory.class, "VanillaWireOutPublisherFactory");
        addAlias(HostIdConnectionStrategy.class, "HostIdConnectionStrategy");
        addAlias(HeartbeatHandler.Factory.class, "HeartbeatHandlerFactory");
        addAlias(ClusterContext.class, "ClusterContext");
        addAlias(EngineWireNetworkContext.Factory.class, "EngineWireNetworkContextFactory");
        addAlias(EngineNetworkStatsListener.Factory.class, "EngineNetworkStatsListenerFactory");
        addAlias(EngineConnectionManager.Factory.class, "EngineConnectionManagerFactory");
        addAlias(TcpEventHandler.Factory.class, "TcpEventHandlerFactory");
        addAlias(EngineClusterContext.class, "EngineClusterContext");
        addAlias(UberHandler.class, "UberHandler");
        addAlias(MapReplicationHandler.class, "MapReplicationHandler");
        addAlias(HeartbeatHandler.class, "HeartbeatHandler");
        addAlias("software.chronicle.enterprise.queue.QueueSourceReplicationHandler");
        addAlias("software.chronicle.ente§rprise.queue.QueueSyncReplicationHandler");
        addAlias(QueueView.class, "QueueView");
        addAlias(MapView.class, "MapView");
        addAlias(Boolean.class, "boolean");
        addAlias(AuthenticatedKeyValueStore.class, "AuthenticatedKeyValueStore");
        addAlias(ObjectKeyValueStore.class, "ObjectKeyValueStore");

    }

    public static boolean loadDefaultAliases() {

        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class,
                EngineCfg.class,
                JmxCfg.class,
                ServerCfg.class,
                ClustersCfg.class,
                InMemoryMapCfg.class,
                FilePerKeyMapCfg.class,
                ChronicleMapCfg.class,
                MonitorCfg.class);
        return true;
    }

    private String pathName;
    private String name;
    private Class viewType, type, type2;
    private String basePath;
    private WireType wireType = WireType.TEXT;
    @Nullable
    private Boolean putReturnsNull = null,
            removeReturnsNull = null,
            nullOldValueOnUpdateEvent = null,
            endSubscriptionAfterBootstrap = null,
            bootstrap = null;
    private double averageValueSize;
    private long entries;
    private Boolean recurse;
    private boolean sealed = false;
    private String cluster = "cluster";

    private int throttlePeriodMs = 0;
    private boolean dontPersist;
    private long token;

    private RequestContext() {
    }

    public RequestContext(String pathName, String name) {
        this.pathName = pathName;
        this.name = name;
    }

    private static void addAlias(Class type, @NotNull String aliases) {
        ClassAliasPool.CLASS_ALIASES.addAlias(type, aliases);
    }

    private static void addAliasLocal(Class type, @NotNull String aliases) {
        CLASS_ALIASES.addAlias(type, aliases);
    }

    private static void addAlias(String className) {
        Class<?> aClass;
        try {
            aClass = Class.forName(className);
        } catch (ClassNotFoundException ignore) {
            return;
        }
        CLASS_ALIASES.addAlias(aClass, aClass.getSimpleName());
    }

    @NotNull
    public static RequestContext requestContext() {
        return new RequestContext();
    }

    // todo improve this !
    @NotNull
    public static RequestContext requestContext(@NotNull CharSequence uri) {
        return requestContext(uri.toString());
    }

    @NotNull
    public static RequestContext requestContext(@NotNull String uri) {

        int queryPos = uri.indexOf('?');
        String fullName = queryPos >= 0 ? uri.substring(0, queryPos) : uri;
        String query = queryPos >= 0 ? uri.substring(queryPos + 1) : "";
        int lastForwardSlash = fullName.lastIndexOf('/');
        if (lastForwardSlash > 0 && fullName.length() == lastForwardSlash + 1) {
            fullName = fullName.substring(0, fullName.length() - 1);
            lastForwardSlash = fullName.lastIndexOf('/');
        }
        String pathName = lastForwardSlash >= 0 ? fullName.substring(0, lastForwardSlash) : "";
        String name = lastForwardSlash >= 0 ? fullName.substring(lastForwardSlash + 1) : fullName;
        RequestContext requestContext = new RequestContext(pathName, name).queryString(query);
        return requestContext;
    }

    static Class lookupType(@NotNull CharSequence typeName) throws ClassNotFoundException {
        return CLASS_ALIASES.forName(typeName);
    }

    @NotNull
    public RequestContext cluster(String clusterTwo) {
        this.cluster = clusterTwo;
        return this;
    }

    @NotNull
    public String cluster() {
        return this.cluster;
    }

    @NotNull
    public RequestContext seal() {
        sealed = true;
        return this;
    }

    @NotNull
    public Class<SubscriptionCollection> getSubscriptionType() {
        Class elementType = elementType();
        return elementType == TopologicalEvent.class
                ? (Class) TopologySubscription.class
                : elementType == BytesStore.class
                ? (Class) RawKVSSubscription.class
                : (Class) ObjectSubscription.class;
    }

    @NotNull
    public RequestContext queryString(@NotNull String queryString) {
        if (queryString.isEmpty())
            return this;
        WireParser<Void> parser = getWireParser();
        Bytes bytes = Bytes.from(queryString);
        QueryWire wire = new QueryWire(bytes);
        while (bytes.readRemaining() > 0)
            parser.parseOne(wire, null);
        return this;
    }

    @NotNull
    public WireParser<Void> getWireParser() {
        WireParser<Void> parser = new VanillaWireParser<>((s, v, $) -> {
        });
        parser.register(() -> "cluster", (s, v, $) -> v.text(this, (o, x) -> o.cluster = x));
        parser.register(() -> "view", (s, v, $) -> v.text(this, RequestContext::view));
        parser.register(() -> "bootstrap", (s, v, $) -> v.bool(this, (o, x) -> o.bootstrap = x));
        parser.register(() -> "putReturnsNull", (s, v, $) -> v.bool(this, (o, x) -> o.putReturnsNull = x));
        parser.register(() -> "removeReturnsNull", (s, v, $) -> v.bool(this, (o, x) -> o.removeReturnsNull = x));
        parser.register(() -> "nullOldValueOnUpdateEvent",
                (s, v, $) -> v.bool(this, (o, x) -> o.nullOldValueOnUpdateEvent = x));
        parser.register(() -> "basePath", (s, v, $) -> v.text(this, (o, x) -> o.basePath = x));
        parser.register(() -> "viewType", (s, v, $) -> v.typeLiteral(this, (o, x) -> o.viewType = x));
        parser.register(() -> "topicType", (s, v, $) -> v.typeLiteral(this, (o, x) -> o.type = x));
        parser.register(() -> "keyType", (s, v, $) -> v.typeLiteral(this, (o, x) -> o.type = x));
        parser.register(() -> "valueType", (s, v, $) -> v.typeLiteral(this, (o, x) -> o.type2 = x));
        parser.register(() -> "messageType", (s, v, $) -> v.typeLiteral(this, (o, x) -> o.type = x));
        parser.register(() -> "elementType", (s, v, $) -> v.typeLiteral(this, (o, x) -> o.type2 = x));
        parser.register(() -> "endSubscriptionAfterBootstrap", (s, v, $) -> v.bool(this, (o, x) -> o.endSubscriptionAfterBootstrap = x));
        parser.register(() -> "throttlePeriodMs", (s, v, $) -> v.int32(this, (o, x) -> o.throttlePeriodMs = x));

        parser.register(() -> "entries", (s, v, $) -> v.int64(this, (o, x) -> o.entries = x));
        parser.register(() -> "averageValueSize", (s, v, $) -> v.int64(this, (o, x) -> o.averageValueSize = x));
        parser.register(() -> "dontPersist", (s, v, $) -> v.bool(this, (o, x) -> o.dontPersist = x));
        parser.register(() -> "token", (s, v, $) -> v.int64(this, (o, x) -> o.token =
                x));
        return parser;
    }

    @NotNull
    public RequestContext view(@NotNull String viewName) {
        try {
            Class clazz = lookupType(viewName);
            viewType(clazz);

        } catch (ClassNotFoundException iae) {
            throw new IllegalArgumentException("Unknown view name=" + viewName);
        }
        return this;
    }

    @NotNull
    public RequestContext type(Class type) {
        checkSealed();
        this.type = type;
        return this;
    }

    @NotNull
    public RequestContext keyType(Class type) {
        checkSealed();
        this.type = type;
        return this;
    }

    @NotNull
    public Class type() {
        if (type == null)
            return String.class;
        return type;
    }

    @NotNull
    public Class elementType() {
        if (type2 != null)
            return type2;
        return String.class;
    }

    public Class keyType() {
        if (type == null)
            return String.class;
        return type;
    }

    public Class valueType() {
        if (type2 == null)
            return String.class;
        return type2;
    }

    public Class topicType() {

        if (type == null)
            return String.class;
        return type;
    }

    public Class messageType() {
        if (type == null)
            return String.class;
        return type;
    }

    public RequestContext messageType(Class clazz) {
        this.type = clazz;
        return this;
    }

    @NotNull
    public RequestContext valueType(Class type2) {
        checkSealed();
        this.type2 = type2;
        return this;
    }

    @NotNull
    public RequestContext type2(Class type2) {
        checkSealed();
        this.type2 = type2;
        return this;
    }

    public Class type2() {
        if (type == null)
            return String.class;
        return type2;
    }

    @NotNull
    public String fullName() {
        final String s = pathName.isEmpty() ? name : (pathName + "/" + name);
        return s.startsWith("/") ? s : "/" + s;
    }

    @NotNull
    public RequestContext basePath(String basePath) {
        checkSealed();
        this.basePath = basePath;
        return this;
    }

    public String basePath() {
        return basePath;
    }

    public String pathName() {
        return pathName;
    }

    // this will be removed shortly
    @Deprecated()
    @NotNull
    public RequestContext wireType(WireType writeType) {
        checkSealed();
        this.wireType = writeType;
        return this;
    }

    // this will be removed shortly
    @Deprecated()
    public WireType wireType() {
        return wireType;
    }

    public String name() {
        return name;
    }

    public double getAverageValueSize() {
        return averageValueSize;
    }

    @NotNull
    public RequestContext averageValueSize(double averageValueSize) {
        checkSealed();
        this.averageValueSize = averageValueSize;
        return this;
    }

    public long getEntries() {
        return entries;
    }

    @NotNull
    public RequestContext entries(long entries) {
        checkSealed();
        this.entries = entries;
        return this;
    }

    @NotNull
    public RequestContext name(String name) {
        this.name = name;
        return this;
    }

    @NotNull
    public RequestContext viewType(Class assetType) {
        checkSealed();
        this.viewType = assetType;
        return this;
    }

    @Nullable
    public Class viewType() {
        return viewType;
    }

    @NotNull
    public RequestContext fullName(@NotNull String fullName) {
        int dirPos = fullName.lastIndexOf('/');
        this.pathName = dirPos >= 0 ? fullName.substring(0, dirPos) : "";
        this.name = dirPos >= 0 ? fullName.substring(dirPos + 1) : fullName;
        return this;
    }

    @Nullable
    public Boolean putReturnsNull() {
        return putReturnsNull;
    }

    @Nullable
    public Boolean removeReturnsNull() {
        return removeReturnsNull;
    }

    @Nullable
    public RequestContext removeReturnsNull(Boolean removeReturnsNull) {
        this.removeReturnsNull = removeReturnsNull;
        return this;
    }

    @Nullable
    public Boolean nullOldValueOnUpdateEvent() {
        return nullOldValueOnUpdateEvent;
    }

    @Nullable
    public Boolean bootstrap() {
        return bootstrap;
    }

    @NotNull
    public RequestContext bootstrap(boolean bootstrap) {
        checkSealed();
        this.bootstrap = bootstrap;
        return this;
    }

    @NotNull
    public RequestContext endSubscriptionAfterBootstrap(boolean endSubscriptionAfterBootstrap) {
        checkSealed();
        this.endSubscriptionAfterBootstrap = endSubscriptionAfterBootstrap;
        return this;
    }

    public Boolean endSubscriptionAfterBootstrap() {
        return endSubscriptionAfterBootstrap;
    }

    void checkSealed() {
        if (sealed) throw new IllegalStateException();
    }

    @NotNull
    @Override
    public String toString() {
        return "RequestContext{" +
                "pathName='" + pathName + '\'' +
                ", name='" + name + '\'' +
                ", viewType=" + viewType +
                ", type=" + type +
                ", type2=" + type2 +
                ", basePath='" + basePath + '\'' +
                ", wireType=" + wireType +
                ", putReturnsNull=" + putReturnsNull +
                ", removeReturnsNull=" + removeReturnsNull +
                ", bootstrap=" + bootstrap +
                ", averageValueSize=" + averageValueSize +
                ", entries=" + entries +
                ", recurse=" + recurse +
                ", endSubscriptionAfterBootstrap=" + endSubscriptionAfterBootstrap +
                ", throttlePeriodMs=" + throttlePeriodMs +
                ", dontPersist=" + dontPersist +
                '}';
    }

    public Boolean recurse() {
        return recurse;
    }

    @NotNull
    public RequestContext recurse(Boolean recurse) {
        this.recurse = recurse;
        return this;
    }

    @NotNull
    public RequestContext clone() {
        try {
            RequestContext clone = (RequestContext) super.clone();
            clone.sealed = false;
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    @NotNull
    public RequestContext putReturnsNull(Boolean putReturnsNull) {
        this.putReturnsNull = putReturnsNull;
        return this;
    }

    public String toUri() {
        StringBuilder sb = new StringBuilder();
        sb.append(fullName());
        String sep = "?";
        final Class viewType = this.viewType;
        if (viewType != null) {
            String alias = CLASS_ALIASES.nameFor(viewType);
            sb.append(sep).append("view=").append(alias);
            sep = "&";
        }
        if (keyType() != null && keyType() != String.class) {
            sb.append(sep).append("keyType=").append(CLASS_ALIASES.nameFor(keyType()));
            sep = "&";
        }
        if (valueType() != null && valueType() != String.class) {
            sb.append(sep).append("valueType=").append(CLASS_ALIASES.nameFor(valueType()));
            sep = "&";
        }
        if (putReturnsNull() != null) {
            sb.append(sep).append("putReturnsNull=").append(putReturnsNull);
            sep = "&";
        }
        if (removeReturnsNull() != null) {
            sb.append(sep).append("removeReturnsNull=").append(putReturnsNull);
            sep = "&";
        }
        if (bootstrap() != null) {
            sb.append(sep).append("bootstrap=").append(bootstrap);
            sep = "&";
        }
        if (bootstrap() != null) {
            sb.append(sep).append("throttlePeriodMs=").append(throttlePeriodMs);
            sep = "&";
        }
        if (dontPersist()) {
            sb.append(sep).append("dontPersist").append(dontPersist);
            sep = "&";
        }
        return sb.toString();
    }

    public int throttlePeriodMs() {
        return throttlePeriodMs;
    }

    public RequestContext throttlePeriodMs(int throttlePeriodMs) {
        this.throttlePeriodMs = throttlePeriodMs;
        return this;
    }

    public <E> RequestContext elementType(Class<E> eClass) {
        this.type2 = eClass;
        return this;
    }

    public RequestContext topicType(Class topicType) {
        this.type = topicType;
        return this;
    }

    public boolean dontPersist() {
        return dontPersist;
    }

    public RequestContext dontPersist(boolean dontPersist) {
        this.dontPersist = dontPersist;
        return this;
    }

    public long token() {
        return token;
    }

    public RequestContext token(long token) {
        this.token = token;
        return this;
    }

    public enum Operation {
        END_SUBSCRIPTION_AFTER_BOOTSTRAP, BOOTSTRAP;

        public void apply(RequestContext rc) {
            switch (this) {
                case END_SUBSCRIPTION_AFTER_BOOTSTRAP:
                    rc.endSubscriptionAfterBootstrap(true);
                    break;
                case BOOTSTRAP:
                    rc.bootstrap(true);
                    break;
            }
        }
    }
}
