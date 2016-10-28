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

package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.core.annotation.ForceInline;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.util.ThrowingConsumer;
import net.openhft.chronicle.engine.api.collection.ValuesCollection;
import net.openhft.chronicle.engine.api.column.ColumnView;
import net.openhft.chronicle.engine.api.column.MapColumnView;
import net.openhft.chronicle.engine.api.column.QueueColumnView;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.map.SubscriptionKeyValueStore;
import net.openhft.chronicle.engine.api.pubsub.*;
import net.openhft.chronicle.engine.api.query.IndexQueueView;
import net.openhft.chronicle.engine.api.query.ObjectCacheFactory;
import net.openhft.chronicle.engine.api.query.VanillaIndexQueueView;
import net.openhft.chronicle.engine.api.query.VanillaObjectCacheFactory;
import net.openhft.chronicle.engine.api.set.EntrySetView;
import net.openhft.chronicle.engine.api.set.KeySetView;
import net.openhft.chronicle.engine.api.tree.*;
import net.openhft.chronicle.engine.collection.VanillaValuesCollection;
import net.openhft.chronicle.engine.map.*;
import net.openhft.chronicle.engine.map.remote.*;
import net.openhft.chronicle.engine.pubsub.*;
import net.openhft.chronicle.engine.query.QueueConfig;
import net.openhft.chronicle.engine.queue.QueueWrappingColumnView;
import net.openhft.chronicle.engine.session.VanillaSessionProvider;
import net.openhft.chronicle.engine.set.RemoteKeySetView;
import net.openhft.chronicle.engine.set.VanillaKeySetView;
import net.openhft.chronicle.network.ClientSessionProvider;
import net.openhft.chronicle.network.VanillaSessionDetails;
import net.openhft.chronicle.network.api.session.SessionProvider;
import net.openhft.chronicle.network.connection.ClientConnectionMonitor;
import net.openhft.chronicle.network.connection.SocketAddressSupplier;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.threads.Threads;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiPredicate;
import java.util.function.Function;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaAsset implements Asset, Closeable {

    public static final Comparator<Class> CLASS_COMPARATOR = Comparator.comparing(Class::getName);
    public static final String LAST = "{last}";
    private static final Logger LOG = LoggerFactory.getLogger(VanillaAsset.class);
    private static final BiPredicate<RequestContext, Asset> ALWAYS = (rc, asset) -> true;
    final Map<Class, Object> viewMap = new ConcurrentSkipListMap<>(CLASS_COMPARATOR);
    final ConcurrentMap<String, Asset> children = new ConcurrentSkipListMap<>();
    private final Asset parent;
    @NotNull
    private final String name;
    private final Map<Class, SortedMap<String, WrappingViewRecord>> wrappingViewFactoryMap =
            new ConcurrentSkipListMap<>(CLASS_COMPARATOR);
    private final Map<Class, LeafView> leafViewMap = new ConcurrentSkipListMap<>(CLASS_COMPARATOR);
    private String fullName = null;
    private Boolean keyedAsset;

    public VanillaAsset(Asset asset, @NotNull String name) {
        this.parent = asset;
        this.name = name;

        if ("".equals(name)) {
            assert parent == null;
        } else {
//            assert parent != null;
            assert name != null;
        }
        if (parent != null) {
            TopologySubscription parentSubs = parent.findView(TopologySubscription.class);
            if (parentSubs != null && !(parentSubs instanceof RemoteTopologySubscription))
                parentSubs.notifyEvent(AddedAssetEvent.of(parent.fullName(), name));
        }
    }

    void configMapCommon() {
        addWrappingRule(ValuesCollection.class, LAST + " values", VanillaValuesCollection::new, MapView.class);
        addView(SubAssetFactory.class, new VanillaSubAssetFactory());
    }

    public void configMapServer() {
        configMapCommon();
        addWrappingRule(EntrySetView.class, LAST + " VanillaEntrySetView", VanillaEntrySetView::new, MapView.class);
        addWrappingRule(KeySetView.class, LAST + " VanillaKeySetView", VanillaKeySetView::new, MapView.class);
        addWrappingRule(Reference.class, LAST + "reference", MapReference::new, MapView.class);
        addWrappingRule(Replication.class, LAST + "replication", VanillaReplication::new, MapView.class);
        addWrappingRule(Publisher.class, LAST + " MapReference", MapReference::new, MapView.class);
        addWrappingRule(TopicPublisher.class, LAST + " MapTopicPublisher", MapTopicPublisher::new, MapView.class);
        addWrappingRule(MapView.class, LAST + " VanillaMapView", VanillaMapView::new, ObjectKeyValueStore.class);
        addWrappingRule(MapColumnView.class, LAST + "Map ColumnView", MapWrappingColumnView::new,
                MapView.class);

        // storage options
        addLeafRule(ObjectSubscription.class, LAST + " vanilla", MapKVSSubscription::new);
        addLeafRule(RawKVSSubscription.class, LAST + " vanilla", MapKVSSubscription::new);
        addWrappingRule(ObjectKeyValueStore.class, LAST + " VanillaSubscriptionKeyValueStore",
                VanillaSubscriptionKeyValueStore::new, AuthenticatedKeyValueStore.class);

        addLeafRule(AuthenticatedKeyValueStore.class, LAST + " VanillaKeyValueStore", VanillaKeyValueStore::new);
        addLeafRule(SubscriptionKeyValueStore.class, LAST + " VanillaKeyValueStore", VanillaKeyValueStore::new);
        addLeafRule(KeyValueStore.class, LAST + " VanillaKeyValueStore", VanillaKeyValueStore::new);
    }

    public void configMapRemote() {
        configMapCommon();
        addWrappingRule(SimpleSubscription.class, LAST + "subscriber", RemoteSimpleSubscription::new, Reference.class);

        addWrappingRule(EntrySetView.class, LAST + " RemoteEntrySetView", RemoteEntrySetView::new, MapView.class);
        addWrappingRule(KeySetView.class, LAST + " RemoteKeySetView", RemoteKeySetView::new, MapView.class);
        addLeafRule(Publisher.class, LAST + " RemotePublisher", RemotePublisher::new);
        addLeafRule(Reference.class, LAST + "reference", RemoteReference::new);
        addLeafRule(TopicPublisher.class, LAST + " RemoteTopicPublisher", RemoteTopicPublisher::new);

        addWrappingRule(MapView.class, LAST + " RemoteMapView", RemoteMapView::new, ObjectKeyValueStore.class);

        addLeafRule(ObjectKeyValueStore.class, LAST + " RemoteKeyValueStore",
                RemoteKeyValueStore::new);
        addLeafRule(ObjectSubscription.class, LAST + " Remote", RemoteKVSSubscription::new);
    }

    void configQueueCommon() {
        addWrappingRule(Reference.class, LAST + "QueueReference",
                QueueReference::new, QueueView.class);
    }

    public void configQueueServer() {
        configQueueCommon();
        addWrappingRule(Publisher.class, LAST + " QueueReference",
                QueueReference::new, QueueView.class);

        addWrappingRule(TopicPublisher.class, LAST + " QueueTopicPublisher",
                QueueTopicPublisher::new, QueueView.class);

        addLeafRule(ObjectSubscription.class, LAST + " QueueObjectSubscription",
                QueueObjectSubscription::new);

        addWrappingRule(IndexQueueView.class, LAST + " VanillaIndexQueueView",
                VanillaIndexQueueView::new, QueueView.class);

        addLeafRule(QueueView.class, LAST + " ChronicleQueueView", ChronicleQueueView::create);

        addWrappingRule(QueueColumnView.class, LAST + "Queue ColumnView",
                QueueWrappingColumnView::new, QueueView.class);
    }

    public void configQueueRemote() {
        configQueueCommon();
        addLeafRule(QueueView.class, LAST + " RemoteQueueView", RemoteQueueView::new);

        addLeafRule(IndexQueueView.class, LAST + " RemoteIndexQueueView",
                RemoteIndexQueueView::new);
    }

    public void standardStack(boolean daemon, boolean binding) {
        configMapCommon();

        String fullName = fullName();
        HostIdentifier hostIdentifier = findView(HostIdentifier.class);
        if (hostIdentifier != null)
            fullName = "tree-" + hostIdentifier.hostId() + fullName;

        ThreadGroup threadGroup = new ThreadGroup(fullName);
        addView(ThreadGroup.class, threadGroup);
        addLeafRule(EventLoop.class, LAST + " event group", (rc, asset) ->
                Threads.<EventLoop, AssertionError>withThreadGroup(threadGroup, () -> {
                    try {
                        EventLoop eg = new EventGroup(daemon);
                        eg.start();
                        return eg;
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }));
        addView(SessionProvider.class, new VanillaSessionProvider());
    }

    @Deprecated
    public void forServer() {
        forServer(true, s -> master(s, 1), false);
    }

    @Nullable
    static Integer master(String s, int defaultMaster) {
        if (s.startsWith("/proc/connections/cluster/throughput")) {
            final String[] split = s.split("/");
            if (split.length > 5) {
                try {
                    return Integer.valueOf(split[4]);
                } catch (NumberFormatException e) {
                    return defaultMaster;
                }
            }
        }
        return defaultMaster;
    }


    public void forServer(boolean daemon,
                          final Function<String, Integer> uriToHostId,
                          boolean binding) {
        standardStack(daemon, binding);

        configMapServer();

        VanillaAsset queue = (VanillaAsset) acquireAsset("/queue");
        queue.configQueueServer();

        VanillaAsset clusterConnections = (VanillaAsset) acquireAsset(
                "/proc/connections/cluster/throughput");

        clusterConnections.configQueueServer();

        addView(QueueConfig.class, new QueueConfig(uriToHostId, true, null, WireType.BINARY));

        addView(ObjectCacheFactory.class, VanillaObjectCacheFactory.INSTANCE);

        addLeafRule(TopologySubscription.class, LAST + " VanillaTopologySubscription",
                VanillaTopologySubscription::new);

    }

    public void forRemoteAccess(@NotNull String[] hostPortDescriptions,
                                @NotNull WireType wire,
                                @NotNull VanillaSessionDetails sessionDetails,
                                @Nullable ClientConnectionMonitor clientConnectionMonitor) throws AssetNotFoundException {

        standardStack(true, false);
        configMapRemote();

        VanillaAsset queue = (VanillaAsset) acquireAsset("queue");
        queue.configQueueRemote();

        //       addLeafRule(Subscriber.class, LAST + " topic publisher", RemoteSubscription::new);

        addLeafRule(TopologySubscription.class, LAST + " RemoteTopologySubscription",
                RemoteTopologySubscription::new);

        SessionProvider sessionProvider = new ClientSessionProvider(sessionDetails);

        EventLoop eventLoop = findOrCreateView(EventLoop.class);
        eventLoop.start();
        if (getView(TcpChannelHub.class) == null) {

            // used for client fail-over
            final SocketAddressSupplier socketAddressSupplier = new SocketAddressSupplier(hostPortDescriptions, name);

            TcpChannelHub view = Threads.withThreadGroup(findView(ThreadGroup.class),
                    () -> new TcpChannelHub(sessionProvider, eventLoop, wire, name.isEmpty() ? "/" : name,
                            socketAddressSupplier, true, clientConnectionMonitor, HandlerPriority.TIMER));
            addView(TcpChannelHub.class, view);
        }
    }

    public void enableTranslatingValuesToBytesStore() {
        addWrappingRule(ObjectKeyValueStore.class, "{Marshalling} string,string mapView",
                (rc, asset) -> rc.keyType() == String.class && rc.valueType() == String.class,
                VanillaStringStringKeyValueStore::new, AuthenticatedKeyValueStore.class);
        addWrappingRule(ObjectKeyValueStore.class, "{Marshalling} string,marshallable mapView",
                (rc, asset) -> rc.keyType() == String.class && Marshallable.class.isAssignableFrom(rc.valueType()),
                VanillaStringMarshallableKeyValueStore::new, AuthenticatedKeyValueStore.class);

        addLeafRule(RawKVSSubscription.class, LAST + " vanilla",
                MapKVSSubscription::new);
    }

    @Override
    public <W, U> void addWrappingRule(Class<W> viewType, String description, BiPredicate<RequestContext, Asset> predicate, WrappingViewFactory<W, U> factory, Class<U> underlyingType) {
        SortedMap<String, WrappingViewRecord> smap = wrappingViewFactoryMap.computeIfAbsent(viewType, k -> new ConcurrentSkipListMap<>());
        smap.put(description, new WrappingViewRecord(predicate, factory, underlyingType));
    }

    @Override
    public <W, U> void addWrappingRule(Class<W> viewType, String description, WrappingViewFactory<W, U> factory, Class<U> underlyingType) {
        addWrappingRule(viewType, description, ALWAYS, factory, underlyingType);
        leafViewMap.remove(viewType);
    }

    @Override
    public <L> void addLeafRule(Class<L> viewType, String description, LeafViewFactory<L> factory) {
        leafViewMap.put(viewType, new LeafView(description, factory));
    }

    @Nullable
    public <I, U> I createWrappingView(Class viewType, RequestContext rc, @NotNull Asset asset, @Nullable U underling) throws AssetNotFoundException {
        SortedMap<String, WrappingViewRecord> smap = wrappingViewFactoryMap.get(viewType);
        if (smap != null)
            for (WrappingViewRecord wvRecord : smap.values()) {
                if (wvRecord.predicate.test(rc, asset)) {
                    if (underling == null)
                        underling = (U) asset.acquireView(wvRecord.underlyingType, rc);
                    return (I) wvRecord.factory.create(rc, asset, underling);
                }
            }
        if (parent == null)
            return null;
        return ((VanillaAsset) parent).createWrappingView(viewType, rc, asset, underling);
    }

    @Nullable
    public <I> I createLeafView(Class viewType, @NotNull RequestContext rc, Asset asset) throws
            AssetNotFoundException {
        LeafView lv = leafViewMap.get(viewType);
        if (lv != null)
            return (I) lv.factory.create(rc.clone().viewType(viewType), asset);
        if (parent == null)
            return null;
        return ((VanillaAsset) parent).createLeafView(viewType, rc, asset);
    }

    @Override
    public boolean isSubAsset() {
        return false;
    }

    @Override
    public boolean hasChildren() {
        return !children.isEmpty();
    }

    @Override
    public <T extends Throwable> void forEachChild(@NotNull ThrowingConsumer<Asset, T> consumer) throws T {
        for (Asset child : children.values()) {
            consumer.accept(child);
        }
    }

    @Nullable
    @Override
    @ForceInline
    public <V> V getView(@NotNull Class<V> viewType) {
        @SuppressWarnings("unchecked")
        V view = (V) viewMap.get(viewType);
        return view;
    }

    @NotNull
    @Override
    @ForceInline
    public String name() {
        return name;
    }

    @NotNull
    @Override
    public String fullName() {
        if (fullName == null)
            fullName = Asset.super.fullName();
        return fullName;
    }

    @NotNull
    @Override
    public <V> V acquireView(@NotNull Class<V> viewType, @NotNull RequestContext rc) throws
            AssetNotFoundException {

        assert fullName().equals(rc.fullName()) :
                "fullName=" + fullName() + " ,rc.fullName()=" + rc.fullName();

        synchronized (viewMap) {
            V view = getView(viewType);
            if (view != null) {
                return view;
            }
            return Threads.withThreadGroup(findView(ThreadGroup.class), () -> {
                V leafView = createLeafView(viewType, rc, this);

                if (leafView != null)
                    return addView(viewType, leafView);
                V wrappingView = createWrappingView(viewType, rc, this, null);
                if (wrappingView == null)
                    throw new AssetNotFoundException("Unable to classify " + viewType.getName() + " context: " + rc);

                return addView(viewType, wrappingView);
            });
        }
    }

    @Override
    public String dumpRules() {
        Wire text = new TextWire(Wires.acquireBytes());
        if (parent != null) {
            ((VanillaAsset) parent).dumpRules(text);
        }
        dumpRules(text);
        dumpChildRules(text);
        return text.toString();
    }

    private void dumpChildRules(Wire text) {
        for (Asset asset : children.values()) {
            if (asset instanceof VanillaAsset) {
                VanillaAsset vasset = (VanillaAsset) asset;
                if (vasset.leafViewMap.size() + vasset.wrappingViewFactoryMap.size() > 0) {
                    vasset.dumpRules(text);
                }
                vasset.dumpChildRules(text);
            }
        }
    }

    private void dumpRules(Wire wire) {
        wire.bytes().append8bit("---\n");
        wire.write("name").text(fullName())
                .write("leaf").marshallable(w -> {
            for (Map.Entry<Class, LeafView> entry : leafViewMap.entrySet()) {
                w.writeEvent(Class.class, entry.getKey()).leaf(false)
                        .text(entry.getValue().name);
            }
        })
                .write("wrapping").marshallable(w -> {
            for (Map.Entry<Class, SortedMap<String, WrappingViewRecord>> entry : wrappingViewFactoryMap.entrySet()) {
                w.writeEvent(Class.class, entry.getKey()).marshallable(ww -> {
                    for (Map.Entry<String, WrappingViewRecord> recordEntry : entry.getValue().entrySet()) {
                        ww.writeEventName(recordEntry.getKey()).object(Class.class, recordEntry.getValue().underlyingType);
                    }
                });
            }
        });
    }


    private <V> V addView0(Class<V> viewType, V view) {

        if (view instanceof KeyedView)
            keyedAsset = ((KeyedView) view).keyedView();

        Object o = viewMap.putIfAbsent(viewType, view);
        // TODO FIX tests so this works.
//        if (o != null && !o.equals(view))
//            throw new IllegalStateException("Attempt to replace " + viewType + " with " + view + " was " + viewMap.get(viewType));
        return view;
    }

    @Override
    public <V> V addView(Class<V> viewType, V view) {

        if (viewType != ColumnView.class && view instanceof ColumnView)
            addView0(ColumnView.class, (ColumnView) view);
        if (viewType != QueueView.class && view instanceof QueueView)
            addView0(QueueView.class, (QueueView) view);

        return addView0(viewType, view);
    }

    @Override
    public <I> void registerView(Class<I> viewType, I view) {
        viewMap.put(viewType, view);
    }

    @Nullable
    @Override
    public SubscriptionCollection subscription(boolean createIfAbsent) throws AssetNotFoundException {
        return createIfAbsent ? acquireView(ObjectSubscription.class) : getView(ObjectSubscription.class);
    }

    @Override
    public void close() {
        viewMap.values().stream()
                .filter(v -> v instanceof java.io.Closeable)
                .forEach(Closeable::closeQuietly);

        forEachChild(Closeable::close);
    }

    @Override
    public void notifyClosing() {
        viewMap.values().stream()
                .filter(v -> v instanceof Closeable)
                .map(v -> (Closeable) v)
                .forEach(Closeable::notifyClosing);

        forEachChild(Closeable::notifyClosing);
    }

    @Override
    @ForceInline
    public Asset parent() {
        return parent;
    }

    @NotNull
    @Override
    public Asset acquireAsset(@NotNull String childName) {
        if (keyedAsset != Boolean.TRUE) {
            int pos = childName.indexOf('/');
            if (pos == 0) {
                childName = childName.substring(1);
                pos = childName.indexOf('/');
            }
            if (pos > 0) {
                String name1 = childName.substring(0, pos);
                String name2 = childName.substring(pos + 1);
                return getAssetOrANFE(name1).acquireAsset(name2);
            }
        }
        return getAssetOrANFE(childName);
    }

    @Override
    public <V> boolean hasFactoryFor(Class<V> viewType) {
        return leafViewMap.containsKey(viewType) || wrappingViewFactoryMap.containsKey(viewType);
    }

    @Nullable
    private Asset getAssetOrANFE(@NotNull String name) throws AssetNotFoundException {
        Asset asset = children.get(name);
        if (asset == null) {
            asset = createAsset(name);
            if (asset == null)
                throw new AssetNotFoundException(name);
        }
        return asset;
    }

    @Nullable
    protected Asset createAsset(@NotNull String name) {
        assert name.length() > 0;
        return children.computeIfAbsent(name, keyedAsset != Boolean.TRUE
                ? n -> new VanillaAsset(this, name)
                : n -> {
            MapView map = getView(MapView.class);

            if (map != null) {
                SubAssetFactory saFactory = findOrCreateView(SubAssetFactory.class);
                return saFactory.createSubAsset(this, name, map.valueType());
            }

            SubAssetFactory saFactory = findOrCreateView(SubAssetFactory.class);
            return saFactory.createSubAsset(this, name, String.class);
        });
    }

    @Override
    public Asset getChild(String name) {
        return children.get(name);
    }

    @Override
    public void removeChild(String name) {
        Asset removed = children.remove(name);
        if (removed == null) return;
        TopologySubscription topologySubscription = removed.findView(TopologySubscription.class);
        if (topologySubscription != null)
            topologySubscription.notifyEvent(RemovedAssetEvent.of(fullName(), name));
    }

    @NotNull
    @Override
    public String toString() {
        return fullName();
    }

    @Override
    public void getUsageStats(AssetTreeStats ats) {
        ats.addAsset(1, 512);
        for (Object o : viewMap.values()) {
            if (o instanceof KeyValueStore) {
                KeyValueStore kvs = (KeyValueStore) o;
                if (kvs.underlying() == null) {
                    long count = kvs.longSize();
                    ats.addAsset(count, count * 1024);
                    break;
                }
            }
        }
        forEachChild(ca -> ca.getUsageStats(ats));
    }

    static class LeafView extends AbstractMarshallable {
        String name;
        transient LeafViewFactory factory;

        public LeafView(String name, LeafViewFactory factory) {
            this.name = name;
            this.factory = factory;
        }

        public String name() {
            return name;
        }
    }

    static class WrappingViewRecord<W, U> {
        final BiPredicate<RequestContext, Asset> predicate;
        final WrappingViewFactory<W, U> factory;
        final Class<U> underlyingType;

        WrappingViewRecord(BiPredicate<RequestContext, Asset> predicate, WrappingViewFactory<W, U> factory, Class<U> underlyingType) {
            this.predicate = predicate;
            this.factory = factory;
            this.underlyingType = underlyingType;
        }

        @NotNull
        @Override
        public String toString() {
            return "wraps " + underlyingType;
        }
    }
}
