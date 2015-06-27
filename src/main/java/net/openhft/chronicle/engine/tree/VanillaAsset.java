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

import net.openhft.chronicle.core.annotation.ForceInline;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.util.ThrowingAcceptor;
import net.openhft.chronicle.engine.api.collection.ValuesCollection;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.map.SubscriptionKeyValueStore;
import net.openhft.chronicle.engine.api.pubsub.*;
import net.openhft.chronicle.engine.api.session.SessionProvider;
import net.openhft.chronicle.engine.api.set.EntrySetView;
import net.openhft.chronicle.engine.api.set.KeySetView;
import net.openhft.chronicle.engine.api.tree.*;
import net.openhft.chronicle.engine.collection.VanillaValuesCollection;
import net.openhft.chronicle.engine.map.*;
import net.openhft.chronicle.engine.pubsub.VanillaReference;
import net.openhft.chronicle.engine.session.VanillaSessionProvider;
import net.openhft.chronicle.engine.set.VanillaKeySetView;
import net.openhft.chronicle.network.VanillaSessionDetails;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.threads.Threads;
import net.openhft.chronicle.threads.api.EventLoop;
import net.openhft.chronicle.wire.Marshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiPredicate;

import static net.openhft.chronicle.engine.api.tree.RequestContext.requestContext;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaAsset implements Asset, Closeable {
    public static final Comparator<Class> CLASS_COMPARATOR = Comparator.comparing(Class::getName);
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

    private Boolean keyedAsset;

    public VanillaAsset(Asset asset, @NotNull String name) {
        this.parent = asset;
        this.name = name;

        if ("".equals(name)) {
            assert parent == null;
        } else {
            assert parent != null;
            assert name != null;
        }
        if (parent != null) {
            TopologySubscription parentSubs = parent.findView(TopologySubscription.class);
            if (parentSubs != null)
                parentSubs.notifyEvent(AddedAssetEvent.of(parent.fullName(), name));
        }
    }

    public void standardStack(boolean daemon) {

        addWrappingRule(Reference.class, LAST + "reference", VanillaReference::new, MapView.class);
        addWrappingRule(Replication.class, LAST + "replication", VanillaReplication::new, MapView.class);
        addWrappingRule(Publisher.class, LAST + "publisher", VanillaReference::new, MapView.class);
        addWrappingRule(EntrySetView.class, LAST + " entrySet", VanillaEntrySetView::new, MapView.class);
        addWrappingRule(KeySetView.class, LAST + " keySet", VanillaKeySetView::new, MapView.class);
        addWrappingRule(ValuesCollection.class, LAST + " values", VanillaValuesCollection::new, MapView.class);

        addWrappingRule(MapView.class, LAST + " string key maps", VanillaMapView::new, ObjectKeyValueStore.class);



        String fullName = fullName();
        HostIdentifier hostIdentifier = findView(HostIdentifier.class);
        if (hostIdentifier != null)
            fullName = "tree-" + hostIdentifier.hostId() + fullName;

        ThreadGroup threadGroup = new ThreadGroup(fullName);
        addView(ThreadGroup.class, threadGroup);
        addLeafRule(EventLoop.class, LAST + " event group", (rc, asset) ->
                Threads.withThreadGroup(threadGroup, () -> {
                    EventLoop eg = new EventGroup(daemon);
                    eg.start();
                    return eg;
                }));
        addView(SessionProvider.class, new VanillaSessionProvider());
    }

    public void forTesting() {
        forTesting(true);
    }

    public void forTesting(boolean daemon) {
        standardStack(daemon);
        addWrappingRule(TopicPublisher.class, LAST + " topic publisher", VanillaTopicPublisher::new, MapView.class);
        addWrappingRule(Publisher.class, LAST + "publisher", VanillaReference::new, MapView.class);
        addWrappingRule(ObjectKeyValueStore.class, LAST + " authenticated",
                VanillaSubscriptionKeyValueStore::new, AuthenticatedKeyValueStore.class);

        addLeafRule(AuthenticatedKeyValueStore.class, LAST + " vanilla", VanillaKeyValueStore::new);
        addLeafRule(SubscriptionKeyValueStore.class, LAST + " vanilla", VanillaKeyValueStore::new);
        addLeafRule(KeyValueStore.class, LAST + " vanilla", VanillaKeyValueStore::new);

        addLeafRule(ObjectKVSSubscription.class, LAST + " vanilla",
                VanillaKVSSubscription::new);

        addLeafRule(TopologySubscription.class, LAST + " vanilla",
                VanillaTopologySubscription::new);
    }

    public void forRemoteAccess(String hostname, int port) {
        standardStack(true);

        addLeafRule(ObjectKVSSubscription.class, LAST + " Remote",
                RemoteKVSSubscription::new);

        addLeafRule(ObjectKeyValueStore.class, LAST + " Remote AKVS",
                RemoteKeyValueStore::new);
        addWrappingRule(Publisher.class, LAST + "publisher", RemotePublisher::new, MapView.class);
        addWrappingRule(TopicPublisher.class, LAST + " topic publisher", RemoteTopicPublisher::new,
                MapView.class);
        addLeafRule(TopologySubscription.class, LAST + " vanilla",
                RemoteTopologySubscription::new);

        SessionProvider sessionProvider = getView(SessionProvider.class);
        VanillaSessionDetails sessionDetails = new VanillaSessionDetails();
        sessionDetails.setUserId(System.getProperty("user.name"));
        sessionProvider.set(sessionDetails);
        if (getView(TcpChannelHub.class) == null) {
            addView(TcpChannelHub.class,
                    Threads.withThreadGroup(findView(ThreadGroup.class),
                            () -> new TcpChannelHub(sessionProvider, hostname, port)));
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
                VanillaKVSSubscription::new);
    }

    @Override
    public <W, U> void addWrappingRule(Class<W> iClass, String description, BiPredicate<RequestContext, Asset> predicate, WrappingViewFactory<W, U> factory, Class<U> underlyingType) {
        SortedMap<String, WrappingViewRecord> smap = wrappingViewFactoryMap.computeIfAbsent(iClass, k -> new ConcurrentSkipListMap<>());
        smap.put(description, new WrappingViewRecord(predicate, factory, underlyingType));
    }

    @Override
    public <W, U> void addWrappingRule(Class<W> iClass, String description, WrappingViewFactory<W, U> factory, Class<U> underlyingType) {
        addWrappingRule(iClass, description, ALWAYS, factory, underlyingType);
        leafViewFactoryMap.remove(iClass);
    }

    @Override
    public <L> void addLeafRule(Class<L> iClass, String description, LeafViewFactory<L> factory) {
        leafViewFactoryMap.put(iClass, factory);
    }

    @Nullable
    @Override
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
        return parent.createWrappingView(viewType, rc, asset, underling);
    }

    @Nullable
    @Override
    public <I> I createLeafView(Class viewType, RequestContext rc, Asset asset) throws
            AssetNotFoundException {
        LeafViewFactory lvFactory = leafViewFactoryMap.get(viewType);
        if (lvFactory != null)
            return (I) lvFactory.create(rc.clone().viewType(viewType), asset);
        if (parent == null)
            return null;
        return parent.createLeafView(viewType, rc, asset);
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
    public void forEachChild(@NotNull ThrowingAcceptor<Asset, InvalidSubscriberException> consumer) throws InvalidSubscriberException {
        for (Asset child : children.values())
            consumer.accept(child);
    }

    @Nullable
    @Override
    @ForceInline
    public <V> V getView(@NotNull Class<V> vClass) {
        @SuppressWarnings("unchecked")
        V view = (V) viewMap.get(vClass);
        return view;
    }

    @NotNull
    @Override
    @ForceInline
    public String name() {
        return name;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @NotNull
    @Override
    public <V> V acquireView(@NotNull Class<V> viewType, RequestContext rc) throws
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
        if (view instanceof View && ((View) view).keyedView()) {
            keyedAsset = true;
        }
        viewMap.put(viewType, view);
        return view;
    }

    @Override
    public <I> void registerView(Class<I> viewType, I view) {
        viewMap.put(viewType, view);
    }

    @NotNull
    @Override
    public Subscription subscription(boolean createIfAbsent) throws AssetNotFoundException {
        return createIfAbsent ? acquireView(ObjectKVSSubscription.class, requestContext()) : getView(ObjectKVSSubscription.class);
    }

    @Override
    public void close() {
        // do nothing
        viewMap.values().stream().filter(v -> v instanceof Closeable).forEach(v -> {
            try {
                ((java.io.Closeable) v).close();
            } catch (IOException e) {
                // do nothing
            }
        });

        try {
            forEachChild(Closeable::close);
        } catch (InvalidSubscriberException e) {
            e.printStackTrace();
        }
    }

    @Override
    @ForceInline
    public Asset parent() {
        return parent;
    }

    @NotNull
    @Override
    public Asset acquireAsset(@NotNull RequestContext context, @NotNull String fullName) throws AssetNotFoundException {
        if (keyedAsset != Boolean.TRUE) {
            int pos = fullName.indexOf("/");
            if (pos >= 0) {
                String name1 = fullName.substring(0, pos);
                String name2 = fullName.substring(pos + 1);
                return getAssetOrANFE(context, name1).acquireAsset(context, name2);
            }
        }
        return getAssetOrANFE(context, fullName);
    }

    @Override
    public <V> boolean hasFactoryFor(Class<V> viewType) {
        return leafViewFactoryMap.containsKey(viewType) || wrappingViewFactoryMap.containsKey(viewType);
    }

    @Nullable
    private Asset getAssetOrANFE(@NotNull RequestContext context, @NotNull String name) throws AssetNotFoundException {
        Asset asset = children.get(name);
        if (asset == null) {
            asset = createAsset(context, name);
            if (asset == null)
                throw new AssetNotFoundException(name);
        }
        return asset;
    }

    @Nullable
    protected Asset createAsset(@NotNull RequestContext context, @NotNull String name) {
        assert name.length() > 0;
        return children.computeIfAbsent(name, keyedAsset != Boolean.TRUE
                ? n -> new VanillaAsset(this, name)
                : n -> new VanillaSubAsset(context, this, name));
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
