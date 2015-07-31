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

package net.openhft.chronicle.engine.redis;

import net.openhft.chronicle.core.util.SerializableFunction;
import net.openhft.chronicle.engine.api.Updatable;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.Reference;
import net.openhft.chronicle.engine.api.pubsub.TopicPublisher;

import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The purpose is to provide Redis like command which wrap a MapView or a Reference to an underlying store.
 */
public class RedisEmulator {

    public static void append(Reference<String> ref, String toAppend) {
        ref.asyncUpdate(v -> v + toAppend);
    }

    public static void append(MapView<String, String> map, String key, String toAppend) {
        map.asyncUpdateKey(key, v -> v + toAppend);
    }

    public static int bitcount(Reference<BitSet> bits) {
        return bits.applyTo(b -> b.cardinality());
    }

    public static int bitcount(MapView<String, BitSet> map, String key) {
        return map.applyToKey(key, b -> b.cardinality());
    }

    public static int bitpos(Reference<BitSet> bits) {
        return bits.applyTo(b -> b.nextSetBit(0));
    }

    public static int bitpos(Reference<BitSet> bits, int from) {
        return bits.applyTo(b -> b.nextSetBit(from));
    }

    public static <T> T blpop(Reference<BlockingDeque<T>> bd, int timeoutMS) {
        return bd.applyTo(d -> {
            try {
                return d.pollFirst(timeoutMS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                return null;
            }
        });
    }

    public static <T> T brpop(Reference<BlockingDeque<T>> bd, int timeoutMS) {
        return bd.applyTo(d -> {
            try {
                return d.pollLast(timeoutMS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                return null;
            }
        });
    }

    public static <T> T brpoplpush(MapView<String, BlockingDeque<T>> deques, String d1, String d2, int timeoutMS) {
        return deques.applyTo(ds -> {
            try {
                T t = ds.get(d1).pollLast(timeoutMS, TimeUnit.MILLISECONDS);
                ds.get(d2).offer(t);
                return t;
            } catch (InterruptedException e) {
                return null;
            }
        });
    }

    public static long decr(Reference<Long> l) {
        return l.applyTo(v -> v - 1);
    }

    public static long decr(MapView<String, Long> map, String key) {
        return map.applyToKey(key, v -> v - 1);
    }

    public static void del(MapView<String, ?> map, String... keys) {
        map.asyncUpdate(m -> Stream.of(keys).forEach(k -> m.remove(k)));
    }

    public static void echo(Updatable updatable, String message) {
        updatable.asyncUpdate(v -> Logger.getAnonymousLogger().info(message));
    }

    public static boolean exists(MapView<String, ?> map, String key) {
        return map.containsKey(key);
    }

    public static <V> V get(MapView<String, V> map, String key) {
        return map.get(key);
    }

    public static boolean getBit(Reference<BitSet> bits, int index) {
        return bits.applyTo(b -> b.get(index));
    }

    public static String getRange(Reference<String> str, int start, int end) {
        return str.applyTo(s -> s.substring(start, end));
    }

    public static <V> V getSet(Reference<V> v, V newValue) {
        return v.getAndSet(newValue);
    }

    /**
     * Removes the specified fields from the hash stored at key. Specified fields that do not exist within
     * this hash are ignored. If key does not exist, it is treated as an empty hash
     * and this command returns 0.
     *
     * @return Integer reply: the number of fields that were removed from the hash,
     * not including specified but non existing fields.
     */
    public static int hdel(MapView<String, ?> map, String... keys) {
        if(keys.length==1){
            return map.getAndRemove(keys[0]) == null ? 0 : 1;
        }
        map.asyncUpdate(m -> Stream.of(keys).forEach(m::remove));
        return 1;
//        return map.applyTo(m -> {
//            int count = 0;
//            for(String key :keys) {
//                if(m.getAndRemove(key) != null)
//                    count++;
//            }
//            return count;
//        });
    }

    public static boolean hexists(MapView<String, ?> map, String key) {
        return map.containsKey(key);
    }

    /**
     * Returns the value associated with field in the hash stored at key.
     *
     * @return Bulk string reply: the value associated with field,
     * or nil when field is not present in the hash or key does not exist.
     */
    public static <V> V hget(MapView<String, V> map, String key) {
        return map.get(key);
    }

    /**
     * Returns all fields and values of the hash stored at key. In the returned value,
     * every field name is followed by its value,
     * so the length of the reply is twice the size of the hash.
     *
     * Note Redis returns the keys in the same order they were inserted
     * Chronicle returns them in an arbitrary order
     *
     * @reply Array reply: list of fields and their values stored in the hash, or
     * an empty list when key does not exist.
     */
    public static <K, V> void hgetall(MapView<K, V> map, Consumer<Map.Entry<K, V>> entryConsumer) {
        map.entrySet().forEach(entryConsumer);
    }

    /**
     * Increments the number stored at field in the hash stored at key by increment.
     * If key does not exist, a new key holding a hash is created. If field does
     * not exist the value is set to 0 before the operation is performed.
     *
     * @return Integer reply: the value at field after the increment operation.
     */
    public static void hincrby(MapView<String, Long> map, String key, long toAdd) {
        map.asyncUpdateKey(key, v -> v + toAdd);
    }

    public static void hincrbyfloat(MapView<String, Double> map, String key, double toAdd) {
        map.asyncUpdateKey(key, v -> v + toAdd);
    }

    public static <K, V> void hkeys(MapView<K, V> map, Consumer<K> keyConsumer) {
        map.keySet().forEach(keyConsumer);
    }

    public static int hlen(MapView map) {
        return map.size();
    }

    public static Map<String, Object> hmget(MapView<String, Object> map, String... keys) {
        return map.applyTo(m -> {
            Map<String, Object> ret = new LinkedHashMap<String, Object>();
            for (String key : keys) {
                ret.put(key, m.get(key));
            }
            return ret;
        });
    }

    public static void hmset(MapView<String, String> map, String... keyAndValues) {
        map.asyncUpdate(m -> {
            for (int i = 0; i < keyAndValues.length; i += 2)
                map.put(keyAndValues[i], keyAndValues[i + 1]);
        });
    }

    /**
     * Delete all the keys of the currently selected DB. This command never fails.
     * The time-complexity for this operation is O(N), N being the number of keys in the database.
     */
    public static <V> String flushdb(MapView<String, V> map){
        map.clear();
        return "OK";
    }

    /**
     * Return the number of keys in the currently-selected database.
     */
    public static <V> int dbsize(MapView<String, V> map){
        return map.size();
    }

    /**
     * Sets field in the hash stored at key to value. If key does not exist, a new key holding a hash is created.
     * If field already exists in the hash, it is overwritten.
     *
     * @return Integer reply, specifically:
     * 1 if field is a new field in the hash and value was set.
     * 0 if field already exists in the hash and the value was updated.
     */
    public static <V> int hset(MapView<String, V> map, String key, V value) {
        V put = map.getAndPut(key, value);
        return put==null ? 1 : 0;
    }

    public static <V> void hsetnx(MapView<String, V> map, String key, V value) {
        map.putIfAbsent(key, value);
    }

    public static int hstrlen(MapView<String, String> map, String key) {
        return map.applyToKey(key, String::length);
    }

    public static <K, V> void hvals(MapView<K, V> map, Consumer<V> valueConsumer) {
        map.values().forEach(valueConsumer);
    }

    public static long incr(Reference<Long> l) {
        return l.applyTo(v -> v + 1);
    }

    public static long incr(MapView<String, Long> map, String key) {
        return map.applyToKey(key, v -> v + 1);
    }

    public static long incrby(MapView<String, Long> map, String key, long toAdd) {
        return map.applyToKey(key, v -> v + toAdd);
    }


    public static double incrbyfloat(MapView<String, Double> map, String key, double toAdd) {
        return map.applyToKey(key, v -> v + toAdd);
    }

    public static Set<String> keys(MapView<String, ?> map, String pattern) {
        return map.applyTo(m -> m.keySet().stream().filter(k -> k.matches(pattern)).collect(Collectors.toSet()));
    }

    public static <V> V lindex(MapView<String, List<V>> map, String name, int index) {
        return map.applyToKey(name, l -> l.get(index));
    }

    public static <V> void linsert(MapView<String, List<V>> map, String name, boolean after, V pivot, V element) {
        map.asyncUpdateKey(name, l -> {
            int index = l.indexOf(pivot);
            if (index >= 0) {
                if (after) index++;
                l.add(index, element);
            }
            return l;
        });
    }

    public static <V> int llen(MapView<String, List<V>> map, String name) {
        return map.applyToKey(name, l -> l.size());
    }

    public static <V> V lpop(MapView<String, List<V>> map, String name) {
        return map.applyToKey(name, l -> l.remove(0));
    }

    public static <V> void lpush(MapView<String, List<V>> map, String name, V... values) {
        map.asyncUpdateKey(name, l -> {
            l.addAll(0, Arrays.asList(values));
            return l;
        });
    }

    public static <V> void lpushx(MapView<String, List<V>> map, String name, V value) {
        map.computeIfPresent(name, (k, l) -> {
            l.add(value);
            return l;
        });
    }

    public static <V> List<V> lrange(MapView<String, List<V>> map, String name, int start, int stop) {
        return map.applyToKey(name, l -> l.subList(start, stop));
    }

    public static <V> void lset(MapView<String, List<V>> map, String name, int index, V value) {
        map.asyncUpdateKey(name, l -> {
            l.set(index, value);
            return l;
        });
    }

    public static Map<String, Object> mget(MapView<String, Object> map, String... keys) {
        return hmget(map, keys);
    }

    public static void mset(MapView<String, String> map, String... keyAndValues) {
        hmset(map, keyAndValues);
    }

    public static <T, M> void publish(TopicPublisher<T, M> publisher, T topic, M message) {
        publisher.publish(topic, message);
    }

    public static <V> void rename(MapView<String, V> map, String from, String to) {
        map.asyncUpdate(m -> m.put(to, m.remove(from)));
    }

    public static <V> void renamenx(MapView<String, V> map, String from, String to) {
        map.asyncUpdate(m -> m.computeIfAbsent(to, k -> m.remove(from)));
    }

    public static <V> V rpop(MapView<String, Deque<V>> map, String key) {
        return map.applyToKey(key, Deque::removeLast);
    }

    public static <V> V rpoplpush(MapView<String, Deque<V>> deques, String from, String to) {
        return deques.applyTo(ds -> {
            V t = ds.get(from).poll();
            ds.get(to).offer(t);
            return t;
        });
    }

    public static <V> void rpush(MapView<String, Deque<V>> map, String key, V... values) {
        map.asyncUpdateKey(key, d -> {
            d.addAll(Arrays.asList(values));
            return d;
        });
    }
}
