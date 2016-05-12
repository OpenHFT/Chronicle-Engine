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

package net.openhft.chronicle.engine.api.tree;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.engine.HeartbeatHandler;
import net.openhft.chronicle.engine.api.collection.ValuesCollection;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.*;
import net.openhft.chronicle.engine.api.session.Heartbeat;
import net.openhft.chronicle.engine.api.set.EntrySetView;
import net.openhft.chronicle.engine.api.set.KeySetView;
import net.openhft.chronicle.engine.cfg.EngineClusterContext;
import net.openhft.chronicle.engine.cfg.VanillaWireOutPublisherFactory;
import net.openhft.chronicle.engine.fs.EngineConnectionManager;
import net.openhft.chronicle.engine.map.ObjectSubscription;
import net.openhft.chronicle.engine.map.RawKVSSubscription;
import net.openhft.chronicle.engine.query.Filter;
import net.openhft.chronicle.engine.query.Operation.OperationType;
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

import static net.openhft.chronicle.core.pool.ClassAliasPool.CLASS_ALIASES;

/**
 * Created by peter on 24/05/15.
 */
public class RequestContext implements Cloneable {
    static {
        addAlias(QueueView.class, "Queue");
        addAlias(MapView.class, "Map");
        addAlias(MapEvent.class, "MapEvent");
        addAlias(TopologicalEvent.class, "TopologicalEvent");
        addAlias(EntrySetView.class, "EntrySet");
        addAlias(KeySetView.class, "KeySet");
        addAlias(ValuesCollection.class, "Values");
        addAlias(Replication.class, "Replication");
        addAlias(Publisher.class, "Publisher, Pub");
        addAlias(TopicPublisher.class, "TopicPublisher, TopicPub");
        addAlias(ObjectSubscription.class, "Subscription");
        addAlias(TopologySubscription.class, "topologySubscription");
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
        addAlias(EngineConnectionManager.Factory.class, "EngineConnectionManagerFactory");
        addAlias(TcpEventHandler.Factory.class, "TcpEventHandlerFactory");
        addAlias(EngineClusterContext.class, "EngineClusterContext");
        addAlias(UberHandler.class, "UberHandler");
        addAlias(MapReplicationHandler.class, "MapReplicationHandler");
        addAlias(HeartbeatHandler.class, "HeartbeatHandler");
        addAlias("software.chronicle.enterprise.queue.QueueSourceReplicationHandler");
        addAlias("software.chronicle.enterprise.queue.QueueSyncReplicationHandler");
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

    private RequestContext() {
    }

    public RequestContext(String pathName, String name) {
        this.pathName = pathName;
        this.name = name;
    }

    private static void addAlias(Class type, @NotNull String aliases) {
        CLASS_ALIASES.addAlias(type, aliases);
    }

    private static void addAlias(String className) {

        Class<?> aClass = null;
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
        return parser;
    }

    @NotNull
    public RequestContext view(@NotNull String viewName) {
        try {
            Class clazz = lookupType(viewName);
            viewType(clazz);
        } catch (ClassNotFoundException iae) {
            throw new IllegalArgumentException("Unknown view name:" + viewName);
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

    @NotNull
    public RequestContext wireType(WireType writeType) {
        checkSealed();
        this.wireType = writeType;
        return this;
    }

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
        if (viewType != null) {
            sb.append(sep).append("view=").append(CLASS_ALIASES.nameFor(viewType()));
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
