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

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.engine.api.management.ManagementTools;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.*;
import net.openhft.chronicle.engine.map.KVSSubscription;
import net.openhft.chronicle.engine.query.Filter;
import net.openhft.chronicle.engine.tree.QueueView;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;

import static net.openhft.chronicle.engine.api.tree.RequestContext.requestContext;

/**
 * A Hierarchical collection of Assets in a tree structure.
 */
public interface AssetTree extends Closeable {

    /**
     * Get the root asset.  This is useful for adding global rules for constructing views.
     *
     * @return the root Asset.
     */
    @NotNull
    Asset root();

    /**
     * Get an existing Asset by fullName or return null.
     *
     * @param fullName of the Asset
     * @return the Asset or null if it doesn't exist.
     */
    @Nullable
    Asset getAsset(String fullName);

    /**
     * Get or create an asset
     *
     * @param fullName of the asset to obtain
     * @return the asset obtained.
     */
    @NotNull
    Asset acquireAsset(@NotNull String fullName) throws AssetNotFoundException;

    /**
     * Acquire a set view for a URI.
     *
     * @param uri    to search for
     * @param eClass element class.
     * @return the Set.
     * @throws AssetNotFoundException the view could not be constructed.
     */
    @NotNull
    default <E> Set<E> acquireSet(@NotNull String uri, Class<E> eClass) throws AssetNotFoundException {
        return acquireView(requestContext(uri).view("set").type(eClass));
    }

    /**
     * Acquire a map view for a URI.
     *
     * @param uri    to search for
     * @param kClass key class of the map.
     * @param vClass value class of the map.
     * @return the Map.
     * @throws AssetNotFoundException the view could not be constructed.
     */
    @NotNull
    default <K, V> MapView<K, V> acquireMap(@NotNull String uri, Class<K> kClass, Class<V> vClass) throws AssetNotFoundException {
        final RequestContext requestContext = requestContext(uri);

        if (requestContext.bootstrap() != null)
            throw new UnsupportedOperationException("Its not possible to set the bootstrap when " +
                    "acquiring a map");

        return acquireView(requestContext.view("map").type(kClass).type2(vClass));
    }

    /**
     * Acquire a publisher to a single URI.
     *
     * @param uri    to search for
     * @param eClass element class.
     * @return the Set.
     * @throws AssetNotFoundException the view could not be constructed.
     */
    @NotNull
    default <E> Publisher<E> acquirePublisher(@NotNull String uri, Class<E> eClass) throws AssetNotFoundException {
        return acquireView(requestContext(uri).view("pub").elementType(eClass));
    }

    /**
     * Acquire a reference to a single URI
     *
     * @param uri    to search for
     * @param eClass type of the state referenced
     * @return a reference to that state.
     * @throws AssetNotFoundException the view could not be constructed.
     */
    @NotNull
    default <E> Reference<E> acquireReference(@NotNull String uri, Class<E> eClass) throws AssetNotFoundException {
        return acquireView(requestContext(uri).view("ref").type(eClass));
    }

    /**
     * Acquire a Topic Publisher view for a URI. A Topic Publisher can publish to any topic in a
     * group by passing the topic as an additional argument.
     *
     * @param uri          to search for
     * @param topicClass   class of topic descriptions e.g. String.class.
     * @param messageClass class of messages to send on that topic..
     * @return the TopicPublisher
     * @throws AssetNotFoundException the view could not be constructed.
     */
    @NotNull
    default <T, E> TopicPublisher<T, E> acquireTopicPublisher(@NotNull String uri, Class<T> topicClass, Class<E> messageClass) throws AssetNotFoundException {
        return acquireView(requestContext(uri).view("topicPub").type(topicClass).type2(messageClass));
    }

    /**
     * Register a subscriber to a URI with an expected type of message by class. <p></p> e.g. if you
     * subscribe to a type of String.class you get the keys which changed, if you subscribe to
     * MapEvent.class you get all the changes to the map.
     *
     * @param uri        of the asset to subscribe to.
     * @param eClass     the type of event/message to subscribe to
     * @param subscriber to notify which this type of message is sent.
     * @throws AssetNotFoundException the view could not be constructed.
     */
    default <E> void registerSubscriber(@NotNull String uri, Class<E> eClass, Subscriber<E> subscriber) throws AssetNotFoundException {
        RequestContext rc = requestContext(uri).type2(eClass);

        acquireSubscription(rc)
                .registerSubscriber(rc, subscriber, Filter.empty());
    }

    /**
     * Register a topic subscriber to a URI with an expected type of message by topic class and
     * message class
     *
     * @param uri          of the asset to subscribe to.
     * @param topicClass   the type of topic to subscribe to
     * @param messageClass the type of event/message to subscribe to
     * @param subscriber   to notify which this type of message is sent.
     * @throws AssetNotFoundException the view could not be constructed.
     */
    default <T, E> void registerTopicSubscriber(@NotNull String uri, Class<T> topicClass, Class<E> messageClass, TopicSubscriber<T, E> subscriber) throws AssetNotFoundException {
        RequestContext rc = requestContext(uri).keyType(topicClass).valueType(messageClass);
        ((KVSSubscription) acquireSubscription(rc)).registerTopicSubscriber(rc, subscriber);
    }

    /**
     * Acquire the Subscription view for a URI.
     *
     * @param requestContext to find the subscription
     * @return the Subscription
     * @throws AssetNotFoundException the view could not be constructed.
     */
    @NotNull
    default SubscriptionCollection acquireSubscription(@NotNull RequestContext requestContext) throws AssetNotFoundException {
        Asset asset = acquireAsset(requestContext.fullName());
        Class<SubscriptionCollection> subscriptionType = requestContext.getSubscriptionType();
        requestContext.viewType(subscriptionType);
        return asset.acquireView(subscriptionType, requestContext);
    }

    /**
     * Remove a subscription.  The alternative is for the Subscription to throw an
     * IllegalSubscriberException.
     *
     * @param uri        of the asset subscribed to.
     * @param subscriber to remove
     */
    default <E> void unregisterSubscriber(@NotNull String uri, @NotNull Subscriber<E> subscriber) {
        RequestContext rc = requestContext(uri);
        SubscriptionCollection subscription = getSubscription(rc);
        if (subscription == null)
            subscriber.onEndOfSubscription();
        else
            subscription.unregisterSubscriber(subscriber);
    }

    /**
     * Remove a topic subscription.  The alternative is for the Subscription to throw an
     * IllegalSubscriberException.
     *
     * @param uri        of the asset subscribed to.
     * @param subscriber to remove
     */
    default <T, E> void unregisterTopicSubscriber(@NotNull String uri, @NotNull TopicSubscriber<T, E> subscriber) throws AssetNotFoundException {
        RequestContext rc = requestContext(uri).viewType(Subscriber.class);
        SubscriptionCollection subscription = getSubscription(rc);
        if (subscription instanceof KVSSubscription)
            ((KVSSubscription) subscription).unregisterTopicSubscriber(subscriber);
        else
            subscriber.onEndOfSubscription();
    }

    /**
     * Get a Subscription view for a URI if ti exists.  This is useful for unsubscribing.
     *
     * @param requestContext to find the subscription
     * @return the Subscription
     * @throws AssetNotFoundException the view could not be constructed.
     */
    @Nullable
    default SubscriptionCollection getSubscription(@NotNull RequestContext requestContext) {
        Asset asset = getAsset(requestContext.fullName());
        Class<SubscriptionCollection> subscriptionType = requestContext.getSubscriptionType();
        requestContext.viewType(subscriptionType);
        return asset == null ? null : asset.getView(subscriptionType);
    }

    /**
     * Acquire a generic service by URI and serviceType.
     *
     * @param uri         to search for
     * @param serviceType type of the service
     * @return the Service
     * @throws AssetNotFoundException if not found, and could not be created.
     */
    @NotNull
    default <S> S acquireService(@NotNull String uri, Class<S> serviceType) throws AssetNotFoundException {
        return acquireView(requestContext(uri).viewType(serviceType));
    }

    /**
     * Acquire a generic view by ResourceContext.
     *
     * @param requestContext to use to search or create the view.
     * @return the view obtained
     * @throws AssetNotFoundException if the view could not be created.
     */
    @NotNull
    default <E> E acquireView(@NotNull RequestContext requestContext) throws AssetNotFoundException {
        return acquireAsset(requestContext.fullName()).acquireView(requestContext);
    }

    /**
     * Enable JMX management of this Asset Tree
     */
    @NotNull
    default AssetTree enableManagement() {
        ManagementTools.enableManagement(this);
        return this;
    }

    /**
     * Enable JMX management of this Asset Tree
     *
     * @param port to enable a simple web service on
     */
    @NotNull
    default AssetTree enableManagement(int port) {
        ManagementTools.enableManagement(this, port);
        return this;
    }

    /**
     * Disable JMX management of this Asset Tree
     */
    @NotNull
    default AssetTree disableManagement() {
        ManagementTools.disableManagement(this);
        return this;
    }

    AssetTreeStats getUsageStats();

    default <T, M> QueueView<T, M> acquireQueue(String uri, Class<T> typeClass, Class<M> messageClass) {

        final RequestContext requestContext = requestContext(uri);

        if (requestContext.bootstrap() != null)
            throw new UnsupportedOperationException("Its not possible to set the bootstrap when " +
                    "acquiring a queue");

        return acquireView(requestContext.view("queue").type(typeClass).type2(messageClass));
    }
}
