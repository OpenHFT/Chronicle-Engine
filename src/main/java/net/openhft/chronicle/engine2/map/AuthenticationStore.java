package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;
import net.openhft.chronicle.engine2.api.map.PermissionsStore;
import net.openhft.chronicle.engine2.api.map.SubscriptionKeyValueStore;
import net.openhft.chronicle.engine2.session.LocalSession;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Created by daniel on 29/05/15.
 */
public class AuthenticationStore<K, MV, V> implements SubscriptionKeyValueStore<K, MV, V>, Closeable {
    private final PermissionsStore<MV> permissionsStore;
    private final SubscriptionKeyValueStore<K, MV, V> kvStore;
    private Map<String, Permissions> userPermissions = new ConcurrentHashMap<>();
    private String user, password;
    private Predicate<Permissions> writePermission = p -> p.isAuthenticated() && !p.isReadOnly();
    private Predicate<Permissions> readPermission = p -> p.isAuthenticated();

    public AuthenticationStore(RequestContext context){
        Class pStoreClass=null;// =  context.getPermissionStore();
        try {
            permissionsStore = (PermissionsStore)pStoreClass.getConstructor(
                    RequestContext.class).newInstance(context);
        } catch (InstantiationException | IllegalAccessException |
                NoSuchMethodException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }

        Class kvStoreClass=null;// =  context.getKVStore();
        try {
            kvStore = (SubscriptionKeyValueStore)kvStoreClass.getConstructor(
                    RequestContext.class).newInstance(context);
        } catch (InstantiationException | IllegalAccessException |
                NoSuchMethodException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }
    @Override
    public void close() throws IOException {

    }

    @Override
    public V getAndPut(K key, V value) {
        return (V)operateWithPermission(() -> kvStore.getAndPut(key, value), writePermission);
    }

    @Override
    public V getAndRemove(K key) {
        return (V)operateWithPermission(() -> kvStore.getAndRemove(key), writePermission);
    }

    @Override
    public V getUsing(K key, MV value) {
        return (V)operateWithPermission(() -> kvStore.getUsing(key, value), readPermission);
    }

    @Override
    public long size() {
        return (long)operateWithPermission(() -> kvStore.size(), readPermission);
    }

    @Override
    public void keysFor(int segment, Consumer<K> kConsumer) {
        operateWithPermission(i->kvStore.keysFor(segment, kConsumer), readPermission);
    }

    @Override
    public void entriesFor(int segment, Consumer<Entry<K, V>> kvConsumer) {
        operateWithPermission(i->kvStore.entriesFor(segment, kvConsumer), readPermission);
    }

    @Override
    public Iterator<Map.Entry<K, V>> entrySetIterator() {
        return (Iterator<Map.Entry<K, V>>)operateWithPermission(() -> kvStore.size(), readPermission);
    }

    @Override
    public void clear() {
        operateWithPermission(i->kvStore.clear(), writePermission);
    }

    @Override
    public SubscriptionKVSCollection<K, V> subscription(boolean createIfAbsent) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Asset asset() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void underlying(KeyValueStore<K, MV, V> underlying) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public KeyValueStore<K, MV, V> underlying() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public View forSession(LocalSession session, Asset asset) {
        throw new UnsupportedOperationException("todo");
    }

    private Object operateWithPermission(Supplier f, Predicate<Permissions> p) {
        Permissions permissions = userPermissions.computeIfAbsent(user, permissionsStore::get);
        if(p.test(permissions)){
            return f.get();
        }else{
            throw new PermissionDeniedException();
        }
    }

    private void operateWithPermission(Consumer<Void> c, Predicate<Permissions> p) {
        Permissions permissions = userPermissions.computeIfAbsent(user, permissionsStore::get);
        if(p.test(permissions)){
            c.accept(null);
        }else{
            throw new PermissionDeniedException();
        }
    }
}
