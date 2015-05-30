package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.Asset;
import net.openhft.chronicle.engine2.api.Permissions;
import net.openhft.chronicle.engine2.api.VanillaPermissions;
import net.openhft.chronicle.engine2.api.View;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;
import net.openhft.chronicle.engine2.api.map.PermissionsStore;
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
    public SubscriptionKVSCollection<String, Permissions> subscription(boolean createIfAbsent) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void keysFor(int segment, Consumer<String> stringConsumer) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void entriesFor(int segment, Consumer<Entry<String, Permissions>> kvConsumer) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Iterator<Map.Entry<String, Permissions>> entrySetIterator() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Asset asset() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void underlying(KeyValueStore<String, MV, Permissions> underlying) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public KeyValueStore<String, MV, Permissions> underlying() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public View forSession(LocalSession session, Asset asset) {
        throw new UnsupportedOperationException("todo");
    }
}
