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

package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.annotation.ForceInline;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.util.ThrowingAcceptor;
import net.openhft.chronicle.engine.api.collection.ValuesCollection;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.map.SubscriptionKeyValueStore;
import net.openhft.chronicle.engine.api.pubsub.*;
import net.openhft.chronicle.engine.api.set.EntrySetView;
import net.openhft.chronicle.engine.api.set.KeySetView;
import net.openhft.chronicle.engine.api.tree.*;
import net.openhft.chronicle.engine.collection.VanillaValuesCollection;
import net.openhft.chronicle.engine.map.*;
import net.openhft.chronicle.engine.map.remote.RemoteKVSSubscription;
import net.openhft.chronicle.engine.map.remote.RemoteKeyValueStore;
import net.openhft.chronicle.engine.map.remote.RemoteMapView;
import net.openhft.chronicle.engine.map.remote.RemoteTopologySubscription;
import net.openhft.chronicle.engine.pubsub.*;
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
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.Wire;
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
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaAsset implements Asset, Closeable {

    public static final Comparator<Class> CLASS_COMPARATOR = Comparator.comparing(Class::getName);
    private static final Logger LOG = LoggerFactory.getLogger(VanillaAsset.class);
    private static final String LAST = "{last}";
    private static final BiPredicate<RequestContext, Asset> ALWAYS = (rc, asset) -> true;
    final Map<Class, Object> viewMap = new ConcurrentSkipListMap<>(CLASS_COMPARATOR);
    final ConcurrentMap<String, Asset> children = new ConcurrentSkipListMap<>();
    private final Asset parent;
    @NotNull
    private final String name;
    private final Map<Class, SortedMap<String, WrappingViewRecord>> wrappingViewFactoryMap =
            new ConcurrentSkipListMap<>(CLASS_COMPARATOR);
    private final Map<Class, LeafViewFactory> leafViewFactoryMap = new ConcurrentSkipListMap<>(CLASS_COMPARATOR);
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

    public void standardStack(boolean daemon, final Consumer<Throwable> onThrowable) {

        final Asset queue = acquireAsset("queue");
        queue.addWrappingRule(Reference.class, LAST + "reference", QueueReference::new, QueueView
                .class);

        addWrappingRule(Reference.class, LAST + "reference", MapReference::new, MapView.class);
        addWrappingRule(Replication.class, LAST + "replication", VanillaReplication::new, MapView.class);

        addWrappingRule(ValuesCollection.class, LAST + " values", VanillaValuesCollection::new, MapView.class);

        addWrappingRule(MapView.class, LAST + " string key maps", VanillaMapView::new, ObjectKeyValueStore.class);
        addView(SubAssetFactory.class, new VanillaSubAssetFactory());
        String fullName = fullName();
        HostIdentifier hostIdentifier = findView(HostIdentifier.class);
        if (hostIdentifier != null)
            fullName = "tree-" + hostIdentifier.hostId() + fullName;

        ThreadGroup threadGroup = new ThreadGroup(fullName);
        addView(ThreadGroup.class, threadGroup);
        addLeafRule(EventLoop.class, LAST + " event group", (rc, asset) ->
                Threads.withThreadGroup(threadGroup, () -> {
                    EventLoop eg = new EventGroup(daemon, onThrowable);
                    eg.start();
                    return eg;
                }));
        addView(SessionProvider.class, new VanillaSessionProvider());
    }

    public void forServer(final Consumer<Throwable> onThrowable) {
        forServer(true, onThrowable);
    }

    public void forServer(boolean daemon, final Consumer<Throwable> onThrowable) {
        standardStack(daemon, onThrowable);

        final Asset queue = acquireAsset("queue");
        queue.addWrappingRule(Publisher.class, LAST + "reference to a ChronicleQueue",
                QueueReference::new, QueueView.class);

        queue.addWrappingRule(TopicPublisher.class, LAST + "reference to a ChronicleQueue",
                QueueTopicPublisher::new, QueueView.class);

        queue.addLeafRule(ObjectSubscription.class, LAST + " vanilla queue subscription",
                QueueObjectSubscription::new);

        queue.addLeafRule(QueueView.class, LAST + "chronicle queue", ChronicleQueueView::new);

        addWrappingRule(Publisher.class, LAST + "publisher", MapReference::new, MapView.class);
        addWrappingRule(EntrySetView.class, LAST + " entrySet", VanillaEntrySetView::new, MapView.class);

        addWrappingRule(TopicPublisher.class, LAST + " topic publisher", MapTopicPublisher::new, MapView.class);

        addWrappingRule(ObjectKeyValueStore.class, LAST + " authenticated",
                VanillaSubscriptionKeyValueStore::new, AuthenticatedKeyValueStore.class);
        addWrappingRule(KeySetView.class, LAST + " keySet", VanillaKeySetView::new, MapView.class);

        addLeafRule(AuthenticatedKeyValueStore.class, LAST + " vanilla", VanillaKeyValueStore::new);
        addLeafRule(SubscriptionKeyValueStore.class, LAST + " vanilla", VanillaKeyValueStore::new);
        addLeafRule(KeyValueStore.class, LAST + " vanilla", VanillaKeyValueStore::new);

        addLeafRule(ObjectSubscription.class, LAST + " vanilla",
                MapKVSSubscription::new);
        addLeafRule(RawKVSSubscription.class, LAST + " vanilla",
                MapKVSSubscription::new);

        addLeafRule(TopologySubscription.class, LAST + " vanilla",
                VanillaTopologySubscription::new);
    }

    public void forRemoteAccess(@NotNull String[] hostPortDescriptions,
                                @NotNull Function<Bytes, Wire> wire,
                                @NotNull VanillaSessionDetails sessionDetails,
                                @Nullable ClientConnectionMonitor clientConnectionMonitor,
                                final Consumer<Throwable> onThrowable) throws
            AssetNotFoundException {

        standardStack(true, onThrowable);


        addWrappingRule(EntrySetView.class, LAST + " entrySet", RemoteEntrySetView::new, MapView.class);

        addWrappingRule(MapView.class, LAST + " remote key maps", RemoteMapView::new, ObjectKeyValueStore.class);

        addWrappingRule(KeySetView.class, LAST + " remote key maps", RemoteKeySetView::new, MapView.class);


        addLeafRule(ObjectSubscription.class, LAST + " Remote", RemoteKVSSubscription::new);

/*
        //TODO This is incorrect should be RemoteKVSSubscription
        addLeafRule(RemoteKVSSubscription.class, LAST + " vanilla",
                MapKVSSubscription::new);
*/

        //addLeafRule(QueueView.class, LAST + " Remote AKVS", RemoteQueueView::new);


        addLeafRule(ObjectKeyValueStore.class, LAST + " Remote AKVS", RemoteKeyValueStore::new);
        addLeafRule(TopicPublisher.class, LAST + " topic publisher", RemoteTopicPublisher::new);
        //       addLeafRule(Subscriber.class, LAST + " topic publisher", RemoteSubscription::new);
        addLeafRule(Publisher.class, LAST + " topic publisher", RemotePublisher::new);

        addWrappingRule(SimpleSubscription.class, LAST + "subscriber", RemoteSimpleSubscription::new, Reference.class);

        addLeafRule(Reference.class, LAST + "reference", RemoteReference::new);


        //  addWrappingRule(Publisher.class, LAST + " topic publisher", RemotePublisher::new,
        //  MapView.class);


        addLeafRule(TopologySubscription.class, LAST + " vanilla",
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
        addWrappingRule(ObjectKeyValueStore.class, "{Marshalling} string,string map",
                (rc, asset) -> rc.keyType() == String.class && rc.valueType() == String.class,
                VanillaStringStringKeyValueStore::new, AuthenticatedKeyValueStore.class);
        addWrappingRule(ObjectKeyValueStore.class, "{Marshalling} string,marshallable map",
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
        leafViewFactoryMap.remove(viewType);
    }

    @Override
    public <L> void addLeafRule(Class<L> viewType, String description, LeafViewFactory<L> factory) {
        leafViewFactoryMap.put(viewType, factory);
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
        LeafViewFactory lvFactory = leafViewFactoryMap.get(viewType);
        if (lvFactory != null)
            return (I) lvFactory.create(rc.clone().viewType(viewType), asset);
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
    public void forEachChild(@NotNull ThrowingAcceptor<Asset, InvalidSubscriberException> childAcceptor) throws InvalidSubscriberException {
        for (Asset child : children.values())
            childAcceptor.accept(child);
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
    public <V> V addView(Class<V> viewType, V view) {

        if (view instanceof KeyedView)
            keyedAsset = ((KeyedView) view).keyedView();

        viewMap.put(viewType, view);
        return view;
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

        try {
            forEachChild(Closeable::close);
        } catch (InvalidSubscriberException e) {
            LOG.error("", e);
        }
    }

    @Override
    public void notifyClosing() {
        viewMap.values().stream()
                .filter(v -> v instanceof Closeable)
                .map(v -> (Closeable) v)
                .forEach(Closeable::notifyClosing);

        try {
            forEachChild(Closeable::notifyClosing);
        } catch (InvalidSubscriberException e) {
            LOG.error("", e);
        }
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
        return leafViewFactoryMap.containsKey(viewType) || wrappingViewFactoryMap.containsKey(viewType);
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
                        if (map.keyType() != String.class)
                            throw new IllegalStateException("You can only have a SubAsset of a " +
                                    "Map with a String key. KeyType=" + map.keyType());

                        SubAssetFactory saFactory = findOrCreateView(SubAssetFactory.class);
                        return saFactory.createSubAsset(this, name, map.valueType());
                    }

                    SubAssetFactory saFactory = findOrCreateView(SubAssetFactory.class);
                    return saFactory.createSubAsset(this, name, String.class);
                }

        );
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
        try {
            forEachChild(ca -> ca.getUsageStats(ats));
        } catch (InvalidSubscriberException e) {
            throw new AssertionError(e);
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
