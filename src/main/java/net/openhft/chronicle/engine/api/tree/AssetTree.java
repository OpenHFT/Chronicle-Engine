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

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.engine.api.management.ManagementTools;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.*;
import net.openhft.chronicle.engine.map.KVSSubscription;
import net.openhft.chronicle.engine.query.Filter;
import net.openhft.chronicle.engine.tree.QueueView;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
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
        @NotNull final RequestContext requestContext = requestContext(uri);

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
     * Acquire a publisher to a single URI.
     *
     * @param uri    to search for
     * @param eClass element class.
     * @return the Set.
     * @throws AssetNotFoundException the view could not be constructed.
     */
    @NotNull
    default <E> Publisher<E> acquirePublisher(@NotNull String uri, String basePath, Class<E> eClass)
            throws AssetNotFoundException {
        return acquireView(requestContext(uri).view("pub").elementType(eClass).basePath(basePath));
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
     * group by passing the topic as an additional argument. Assumes both String topic and message
     * class
     *
     * @param uri to search for
     * @return the TopicPublisher
     * @throws AssetNotFoundException the view could not be constructed.
     */
    @NotNull
    default <T, E> TopicPublisher<T, E> acquireTopicPublisher(@NotNull String uri) throws AssetNotFoundException {
        return acquireView(requestContext(uri).view("topicPub"));
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
     * Acquire a Topic Publisher view for a URI. A Topic Publisher can publish to any topic in a
     * group by passing the topic as an additional argument.
     *
     * @param uri          to search for
     * @param basePath     the location the topic is pushed to on the file system
     * @param topicClass   class of topic descriptions e.g. String.class.
     * @param messageClass class of messages to send on that topic..
     * @return the TopicPublisher
     * @throws AssetNotFoundException the view could not be constructed.
     */
    @NotNull
    default <T, E> TopicPublisher<T, E> acquireTopicPublisher(@NotNull String uri, String basePath,
                                                              Class<T> topicClass, Class<E> messageClass) throws AssetNotFoundException {
        return acquireView(requestContext(uri).view("topicPub").type(topicClass).type2
                (messageClass).basePath(basePath));
    }

    @NotNull
    default <T, E> TopicPublisher<T, E> acquireTopicPublisher(@NotNull String uri, File baseDir,
                                                              Class<T> topicClass, Class<E> messageClass)
            throws AssetNotFoundException {
        return acquireTopicPublisher(uri, baseDir.getAbsolutePath(), topicClass, messageClass);
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
    default <E> void registerSubscriber(@NotNull String uri, Class<E> eClass, @NotNull Subscriber<E> subscriber) throws AssetNotFoundException {
        @NotNull RequestContext rc = requestContext(uri).type2(eClass);

        acquireSubscription(rc)
                .registerSubscriber(rc, subscriber, Filter.empty());
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
    default <E> void registerSubscriber(@NotNull String uri, String basePath, Class<E> eClass,
                                        @NotNull Subscriber<E> subscriber) throws AssetNotFoundException {
        @NotNull RequestContext rc = requestContext(uri).type2(eClass).basePath(basePath);

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
    default <T, E> void registerTopicSubscriber(@NotNull String uri, Class<T> topicClass, Class<E> messageClass, @NotNull TopicSubscriber<T, E> subscriber) throws AssetNotFoundException {
        @NotNull RequestContext rc = requestContext(uri).keyType(topicClass).valueType(messageClass);
        @NotNull final KVSSubscription kvsSubscription = (KVSSubscription) acquireSubscription(rc);
        kvsSubscription.registerTopicSubscriber(rc, subscriber);
    }

    /**
     * Register a topic subscriber to a URI with an expected type of message by topic class and
     * message class
     *
     * @param uri          of the asset to subscribe to.
     * @param basePath     the path the asset is stored to
     * @param topicClass   the type of topic to subscribe to
     * @param messageClass the type of event/message to subscribe to
     * @param subscriber   to notify which this type of message is sent.
     * @throws AssetNotFoundException the view could not be constructed.
     */
    default <T, E> void registerTopicSubscriber(@NotNull String uri, String basePath, Class<T> topicClass,
                                                Class<E> messageClass, @NotNull TopicSubscriber<T, E> subscriber) throws AssetNotFoundException {
        @NotNull RequestContext rc = requestContext(uri).keyType(topicClass).valueType(messageClass)
                .basePath(basePath);
        @NotNull final KVSSubscription kvsSubscription = (KVSSubscription) acquireSubscription(rc);
        kvsSubscription.registerTopicSubscriber(rc, subscriber);
    }

    default <T, E> void registerTopicSubscriber(@NotNull String uri, File baseFile, Class<T> topicClass,
                                                Class<E> messageClass, @NotNull TopicSubscriber<T, E> subscriber)
            throws AssetNotFoundException {
        registerTopicSubscriber(uri, baseFile.getAbsolutePath(), topicClass, messageClass, subscriber);
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
        @NotNull Asset asset = acquireAsset(requestContext.fullName());
        @NotNull Class<SubscriptionCollection> subscriptionType = requestContext.getSubscriptionType();
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
        @NotNull RequestContext rc = requestContext(uri);
        @Nullable SubscriptionCollection subscription = getSubscription(rc);
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
        @NotNull RequestContext rc = requestContext(uri).viewType(Subscriber.class);
        @Nullable SubscriptionCollection subscription = getSubscription(rc);
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
        @Nullable Asset asset = getAsset(requestContext.fullName());
        @NotNull Class<SubscriptionCollection> subscriptionType = requestContext.getSubscriptionType();
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

        @NotNull final Asset asset = acquireAsset(requestContext.fullName());

     /*   if (requestContext.viewType() == MapView.class
                && asset.findView(MapView.class) == null) {

            final QueueView queueView = asset.findView(QueueView.class);
            if (queueView != null)
                return (E) queueView();

        }*/

        return asset.acquireView(requestContext);
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

    @NotNull
    default <T, M> QueueView<T, M> acquireQueue(@NotNull String uri, Class<T> typeClass, Class<M> messageClass, final String cluster) {

        @NotNull final RequestContext requestContext = requestContext(uri);

        if (requestContext.bootstrap() != null)
            throw new UnsupportedOperationException("Its not possible to set the bootstrap when " +
                    "acquiring a queue");

        return acquireView(requestContext.view("queue").type(typeClass).type2(messageClass)
                .cluster(cluster));
    }

    @NotNull
    default <T, M> QueueView<T, M> acquireQueue(@NotNull String uri, Class<T> typeClass, Class<M>
            messageClass, final String cluster, String basePath) {

        @NotNull final RequestContext requestContext = requestContext(uri).basePath(basePath);

        if (requestContext.bootstrap() != null)
            throw new UnsupportedOperationException("Its not possible to set the bootstrap when " +
                    "acquiring a queue");

        return acquireView(requestContext.view("queue").type(typeClass).type2(messageClass)
                .cluster(cluster));
    }

    @NotNull
    default <T, M> QueueView<T, M> acquireQueue(@NotNull String uri, String basePath, Class<T> typeClass,
                                                Class<M> messageClass, final String cluster) {

        @NotNull final RequestContext requestContext = requestContext(uri).basePath(basePath);

        if (requestContext.bootstrap() != null)
            throw new UnsupportedOperationException("Its not possible to set the bootstrap when " +
                    "acquiring a queue");

        return acquireView(requestContext.view("queue").type(typeClass).type2(messageClass)
                .cluster(cluster));
    }

}
