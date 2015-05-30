package net.openhft.chronicle.engine2;

import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.session.VanillaAsset;
import net.openhft.chronicle.engine2.session.VanillaSession;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by peter on 22/05/15.
 */
public enum Chassis {
    /* no instances */;
    private static volatile Session session;

    static {
        resetChassis();
    }

    public static void resetChassis() {
        session = new VanillaSession();
    }
    public static void defaultSession(Session session) {
        Chassis.session = session;
    }

    public static Session defaultSession() {
        return session;
    }

    public static <E> Set<E> acquireSet(String name, Class<E> eClass) throws AssetNotFoundException {
        return session.acquireSet(name, eClass);
    }

    public static <K, V> ConcurrentMap<K, V> acquireMap(String name, Class<K> kClass, Class<V> vClass) throws AssetNotFoundException {
        return session.acquireMap(name, kClass, vClass);
    }

    public static <E> Reference<E> acquireReference(String name, Class<E> eClass) throws AssetNotFoundException {
        return session.acquireReference(name, eClass);
    }

    public static <E> Publisher<E> acquirePublisher(String name, Class<E> eClass) throws AssetNotFoundException {
        return session.acquirePublisher(name, eClass);
    }

    public static <T, E> TopicPublisher<T, E> acquireTopicPublisher(String name, Class<T> tClass, Class<E> eClass) throws AssetNotFoundException {
        return session.acquireTopicPublisher(name, tClass, eClass);
    }

    public static <E> void registerSubscriber(String name, Class<E> eClass, Subscriber<E> subscriber) throws AssetNotFoundException {
        session.registerSubscriber(name, eClass, subscriber);
    }

    public static <E> void unregisterSubscriber(String name, Class<E> eClass, Subscriber<E> subscriber) {
        session.unregisterSubscriber(name, eClass, subscriber);
    }

    public static <T, E> void registerTopicSubscriber(String name, Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber) throws AssetNotFoundException {
        session.registerTopicSubscriber(name, tClass, eClass, subscriber);
    }

    public static <T, E> void unregisterTopicSubscriber(String name, Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber) {
        session.unregisterTopicSubscriber(name, tClass, eClass, subscriber);
    }

    public static <E> void registerFactory(String name, Class<E> eClass, Factory<E> factory) {
        session.registerFactory(name, eClass, factory);
    }

    public static void viewTypeLayersOn(Class viewType, String description, Class underlyingType) {
        ((VanillaSession) session).viewTypeLayersOn(viewType, description, underlyingType);
    }

    public static void enableTranslatingValuesToBytesStore() {
        ((VanillaAsset) session.getAsset("")).enableTranslatingValuesToBytesStore();
    }

    public static Asset getAsset(String name) {
        return session.getAsset(name);
    }

    public static Asset addAsset(String name, Assetted item) {
        return session.add(name, item);
    }

    public static <A> Asset acquireAsset(String name, Class<A> assetClass, Class class1, Class class2) {
        return session.acquireAsset(assetClass, RequestContext.requestContext(name).type(class1).type2(class2));
    }
}
