package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.api.map.PermissionsStore;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;
import net.openhft.chronicle.engine2.session.LocalSession;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Created by daniel on 29/05/15.
 */
public class VanillaPermissionsStore<MV> implements PermissionsStore<MV> {

    private Map<String, Permissions> userPermissions = new ConcurrentHashMap<>();

    @Override
    public Permissions getAndPut(String key, Permissions value) {
       return userPermissions.put(key, value);
    }

    @Override
    public Permissions getAndRemove(String key) {
        return userPermissions.remove(key);
    }

    @Override
    public Permissions getUsing(String key, MV value) {
        if(value != null)throw new UnsupportedOperationException("Mutable values not supported");
        return userPermissions.computeIfAbsent(key, s -> {
            VanillaPermissions vanillaPermissions = new VanillaPermissions();
            vanillaPermissions.setAuthenticated(true);
            vanillaPermissions.setReadOnly(true);
            return vanillaPermissions;
        });
    }

    @Override
    public long size() {
        return userPermissions.size();
    }

    @Override
    public void keysFor(int segment, Consumer<String> stringConsumer) {

    }

    @Override
    public void entriesFor(int segment, Consumer<Entry<String, Permissions>> kvConsumer) {

    }

    @Override
    public Iterator<Map.Entry<String, Permissions>> entrySetIterator() {
        return null;
    }

    @Override
    public void clear() {

    }

    @Override
    public void asset(Asset asset) {

    }

    @Override
    public Asset asset() {
        return null;
    }

    @Override
    public void underlying(KeyValueStore<String, MV, Permissions> underlying) {

    }

    @Override
    public KeyValueStore<String, MV, Permissions> underlying() {
        return null;
    }

    @Override
    public <E> void registerSubscriber(RequestContext rc, Subscriber<E> subscriber) {

    }

    @Override
    public <T, E> void registerTopicSubscriber(RequestContext rc, TopicSubscriber<T, E> subscriber) {

    }

    @Override
    public void unregisterSubscriber(RequestContext rc, Subscriber subscriber) {

    }

    @Override
    public void unregisterTopicSubscriber(RequestContext rc, TopicSubscriber subscriber) {

    }

    @Override
    public View forSession(LocalSession session, Asset asset) {
        return null;
    }
}
