/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.client.internal.ChronicleEngine;
import net.openhft.chronicle.engine.map.MapClientTest.RemoteMapSupplier;
import net.openhft.chronicle.map.ChronicleMap;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.*;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 * Other contributors include Andrew Wright, Jeffrey Hayes,
 * Pat Fisher, Mike Judd.
 */

public class RemoteChronicleMapTest extends JSR166TestCase {


    static ChronicleMap<Integer, String> newIntString() throws IOException {
        final RemoteMapSupplier remoteMapSupplier = new RemoteMapSupplier(Integer.class, String.class, new ChronicleEngine());

        final ChronicleMap spy = Mockito.spy(remoteMapSupplier.get());

        Mockito.doAnswer(invocationOnMock -> {
            remoteMapSupplier.close();
            return null;
        }).when(spy).close();

        return spy;
    }

    static ChronicleMap<CharSequence, CharSequence> newStrStrMap() throws
            IOException {

        final RemoteMapSupplier remoteMapSupplier = new RemoteMapSupplier(CharSequence.class, CharSequence.class, new ChronicleEngine());
        final ChronicleMap spy = Mockito.spy(remoteMapSupplier.get());

        Mockito.doAnswer(invocationOnMock -> {
            remoteMapSupplier.close();
            return null;
        }).when(spy).close();

        return spy;
    }

    static ChronicleMap<byte[], byte[]> newByteArrayMap() throws IOException {
        final RemoteMapSupplier remoteMapSupplier = new RemoteMapSupplier(byte[]
                .class, byte[]
                .class, new ChronicleEngine());
        final ChronicleMap spy = Mockito.spy(remoteMapSupplier.get());

        Mockito.doAnswer(invocationOnMock -> {
            remoteMapSupplier.close();
            return null;
        }).when(spy).close();

        return spy;

    }

    /**
     * Returns a new map from Integers 1-5 to Strings "A"-"E".
     */
    private ChronicleMap<Integer, String> map5() throws IOException {
        ChronicleMap<Integer, String> map = newIntString();
        assertTrue(map.isEmpty());
        map.put(one, "A");
        map.put(two, "B");
        map.put(three, "C");
        map.put(four, "D");
        map.put(five, "E");
        assertFalse(map.isEmpty());
        assertEquals(5, map.size());
        return map;
    }

    static int s_port = 11050;

    @Test(timeout = 30000)
    @Ignore
    public void testByteArrayPerf() throws IOException {
        ByteBuffer key = ByteBuffer.allocate(8);
        ByteBuffer value = ByteBuffer.allocate(50);
        int runs = 100000;
        try (ChronicleMap<byte[], byte[]> map = newByteArrayMap()) {
            for (int t = 0; t < 5; t++) {
                {
                    long start = System.nanoTime();
                    for (int i = 0; i < runs; i++) {
                        key.putLong(0, i);
                        value.putLong(0, i);
                        map.put(key.array(), value.array());
                    }
                    long time = System.nanoTime() - start;
                    System.out.printf("%d: Took %.1f us on average to put %n", t, time / 1e3 / runs);
                }
                {
                    long start = System.nanoTime();
                    for (int i = 0; i < runs; i++) {
                        key.putLong(0, i);
                        byte[] b = map.get(key.array());
                    }
                    long time = System.nanoTime() - start;
                    System.out.printf("%d: Took %.1f us on average to get %n", t, time / 1e3 / runs);
                }
            }
        }
    }

    /**
     * clear removes all pairs
     */
    @Test(timeout = 50000)
    public void testClear() throws IOException {
        try (ChronicleMap<Integer, String> map = map5()) {
            map.clear();
            assertEquals(0, map.size());
        }
    }

    /**
     * contains returns true for contained value
     */
    @Test(timeout = 50000)
    public void testContains() throws IOException {
        try (ChronicleMap map = map5()) {
            assertTrue(map.containsValue("A"));
            assertFalse(map.containsValue("Z"));
        }
    }

    /**
     * containsKey returns true for contained key
     */
    @Test(timeout = 50000)
    public void testContainsKey() throws IOException {
        try (ChronicleMap map = map5()) {
            assertTrue(map.containsKey(one));
            assertFalse(map.containsKey(zero));
        }
    }

    /**
     * containsValue returns true for held values
     */
    @Test(timeout = 50000)
    public void testContainsValue() throws IOException {
        try (ChronicleMap map = map5()) {
            assertTrue(map.containsValue("A"));
            assertFalse(map.containsValue("Z"));
        }
    }

    /**
     * get returns the correct element at the given key, or null if not present
     */
    @Test(timeout = 50000)
    public void testGet() throws IOException {
        try (ChronicleMap map = map5()) {
            assertEquals("A", (String) map.get(one));
            try (ChronicleMap empty = newStrStrMap()) {
                assertNull(map.get(notPresent));
            }
        }
    }

    /**
     * isEmpty is true of empty map and false for non-empty
     */
    @Test(timeout = 50000)
    public void testIsEmpty() throws IOException {
        try (ChronicleMap empty = newIntString()) {
            try (ChronicleMap map = map5()) {
                if (!empty.isEmpty()) {
                    System.out.print("not empty " + empty);
                }
                assertTrue(empty.isEmpty());
                assertFalse(map.isEmpty());
            }
        }
    }

    /**
     * keySet returns a Set containing all the keys
     */

    @Test(timeout = 50000)
    public void testKeySet() throws IOException {
        try (ChronicleMap map = map5()) {
            Set s = map.keySet();
            assertEquals(5, s.size());
            assertTrue(s.contains(one));
            assertTrue(s.contains(two));
            assertTrue(s.contains(three));
            assertTrue(s.contains(four));
            assertTrue(s.contains(five));
        }
    }

    /**
     * keySet.toArray returns contains all keys
     */
    @Ignore
    @Test(timeout = 50000)
    public void testKeySetToArray() throws IOException {
        try (ChronicleMap map = map5()) {
            Set s = map.keySet();
            Object[] ar = s.toArray();
            assertTrue(s.containsAll(Arrays.asList(ar)));
            assertEquals(5, ar.length);
            ar[0] = m10;
            assertFalse(s.containsAll(Arrays.asList(ar)));
        }
    }

    /**
     * Values.toArray contains all values
     */
    @Ignore
    @Test(timeout = 50000)
    public void testValuesToArray() throws IOException {
        try (ChronicleMap map = map5()) {
            Collection v = map.values();
            Object[] ar = v.toArray();
            ArrayList s = new ArrayList(Arrays.asList(ar));
            assertEquals(5, ar.length);
            assertTrue(s.contains("A"));
            assertTrue(s.contains("B"));
            assertTrue(s.contains("C"));
            assertTrue(s.contains("D"));
            assertTrue(s.contains("E"));
        }
    }

    /**
     * entrySet.toArray contains all entries
     */
    @Ignore
    @Test(timeout = 50000)
    public void testEntrySetToArray() throws IOException {
        try (ChronicleMap map = map5()) {
            Set s = map.entrySet();
            Object[] ar = s.toArray();
            assertEquals(5, ar.length);
            for (int i = 0; i < 5; ++i) {
                assertTrue(map.containsKey(((Map.Entry) (ar[i])).getKey()));
                assertTrue(map.containsValue(((Map.Entry) (ar[i])).getValue()));
            }
        }
    }

    /**
     * values collection contains all values
     */
    @Ignore
    @Test(timeout = 50000)
    public void testValues() throws IOException {
        try (ChronicleMap map = map5()) {
            Collection s = map.values();
            assertEquals(5, s.size());
            assertTrue(s.contains("A"));
            assertTrue(s.contains("B"));
            assertTrue(s.contains("C"));
            assertTrue(s.contains("D"));
            assertTrue(s.contains("E"));
        }
    }

    /**
     * entrySet contains all pairs
     */

    @Test
    public void testEntrySet() throws IOException {
        try (ChronicleMap map = map5()) {
            Set s = map.entrySet();
            assertEquals(5, s.size());
            Iterator it = s.iterator();
            while (it.hasNext()) {
                Map.Entry e = (Map.Entry) it.next();
                assertTrue(
                        (e.getKey().equals(one) && e.getValue().equals("A")) ||
                                (e.getKey().equals(two) && e.getValue().equals("B")) ||
                                (e.getKey().equals(three) && e.getValue().equals("C")) ||
                                (e.getKey().equals(four) && e.getValue().equals("D")) ||
                                (e.getKey().equals(five) && e.getValue().equals("E"))
                );
            }
        }
    }

    /**
     * putAll adds all key-value pairs from the given map
     */

    @Test(timeout = 50000)
    public void testPutAll() throws IOException {
        int port = s_port++;
        try (ChronicleMap empty = newIntString()) {
            try (ChronicleMap map = map5()) {
                empty.putAll(map);
                assertEquals(5, empty.size());
                assertTrue(empty.containsKey(one));
                assertTrue(empty.containsKey(two));
                assertTrue(empty.containsKey(three));
                assertTrue(empty.containsKey(four));
                assertTrue(empty.containsKey(five));
            }
        }
    }

    /**
     * putIfAbsent works when the given key is not present
     */
    @Test(timeout = 50000)
    public void testPutIfAbsent() throws IOException {
        try (ChronicleMap map = map5()) {
            map.putIfAbsent(six, "Z");
            assertTrue(map.containsKey(six));
        }
    }

    /**
     * putIfAbsent does not add the pair if the key is already present
     */
    @Test(timeout = 50000)
    public void testPutIfAbsent2() throws IOException {
        try (ChronicleMap map = map5()) {
            assertEquals("A", map.putIfAbsent(one, "Z"));
        }
    }

    /**
     * replace fails when the given key is not present
     */
    @Test(timeout = 50000)
    public void testReplace() throws IOException {
        try (ChronicleMap map = map5()) {
            assertNull(map.replace(six, "Z"));
            assertFalse(map.containsKey(six));
        }
    }

    /**
     * replace succeeds if the key is already present
     */
    @Test(timeout = 50000)
    public void testReplace2() throws
            IOException {
        try (ChronicleMap map = map5()) {
            assertNotNull(map.replace(one, "Z"));
            assertEquals("Z", map.get(one));
        }
    }

    /**
     * replace value fails when the given key not mapped to expected value
     */
    @Test(timeout = 50000)
    public void testReplaceValue() throws IOException {
        try (ChronicleMap map = map5()) {
            assertEquals("A", map.get(one));
            assertFalse(map.replace(one, "Z", "Z"));
            assertEquals("A", map.get(one));
        }
    }

    /**
     * replace value succeeds when the given key mapped to expected value
     */
    @Test(timeout = 50000)
    public void testReplaceValue2() throws IOException {
        try (ChronicleMap map = map5()) {
            assertEquals("A", map.get(one));
            assertTrue(map.replace(one, "A", "Z"));
            assertEquals("Z", map.get(one));
        }
    }

    /**
     * remove removes the correct key-value pair from the map
     */
    @Test(timeout = 50000)
    public void testRemove() throws IOException {
        try (ChronicleMap map = map5()) {
            map.remove(five);
            assertEquals(4, map.size());
            assertFalse(map.containsKey(five));
        }
    }

    /**
     * remove(key,value) removes only if pair present
     */
    @Test(timeout = 50000)
    public void testRemove2
    () throws IOException {
   /*     try(   ChronicleMap map = map5(8076)) {
        map.remove(five, "E");
    assertEquals(4, map.size());
        assertFalse(map.containsKey(five));
        map.remove(four, "A");
        assertEquals(4, map.size());
        assertTrue(map.containsKey(four));
   */
    }

    /**
     * size returns the correct values
     */
    @Test(timeout = 50000)
    public void testSize() throws IOException {
        try (ChronicleMap map = map5()) {
            try (ChronicleMap empty = newIntString()) {
                assertEquals(0, empty.size());
                assertEquals(5, map.size());
            }
        }
    }

    /**
     * size returns the correct values
     */
    @Test(timeout = 150000)
    public void testSize2() throws IOException {
        try (ChronicleMap map = map5()) {
            try (ChronicleMap empty = newIntString()) {
                assertEquals(0, empty.size());
                assertEquals(5, map.size());
            }
        }
    }

    /**
     * size returns the correct values
     */
    @Test(timeout = 50000)
    public void testSize3() throws IOException {
        try (ChronicleMap map = map5()) {
            try (ChronicleMap empty = newIntString()) {
                assertEquals(0, empty.size());
                assertEquals(5, map.size());
            }
        }
    }


    /**
     * get(null) throws NPE
     */
    @Ignore
    @Test(timeout = 50000)
    public void testGet_NullPointerException() throws IOException {

        try (ChronicleMap c = newIntString()) {
            c.get(null);
            shouldThrow();
        } catch (NullPointerException success) {
        } catch (IllegalArgumentException success) {
        }
    }

    /**
     * containsKey(null) throws NPE
     */
    @Test(timeout = 50000)
    public void testContainsKey_NullPointerException() throws IOException {
        try (ChronicleMap c = newIntString()) {
            c.containsKey(null);
            shouldThrow();
        } catch (NullPointerException success) {
        } catch (IllegalArgumentException success) {
        }
    }

    /**
     * put(null,x) throws NPE
     */
    @Test(timeout = 50000)
    public void testPut1_NullPointerException() throws IOException {
        try (ChronicleMap c = newIntString()) {
            c.put(null, "whatever");
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * put(x, null) throws NPE
     */
    @Test(timeout = 50000)
    public void testPut2_NullPointerException() throws IOException {
        try (ChronicleMap c = newIntString()) {
            c.put(notPresent, null);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * putIfAbsent(null, x) throws NPE
     */
    @Test(timeout = 50000)
    public void testPutIfAbsent1_NullPointerException() throws IOException {
        try (ChronicleMap c = newIntString()) {
            c.putIfAbsent(null, "whatever");
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * replace(null, x) throws NPE
     */
    @Test(timeout = 50000)
    public void testReplace_NullPointerException() throws IOException {
        try (ChronicleMap c = newIntString()) {
            c.replace(null, "whatever");
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * replace(null, x, y) throws NPE
     */
    @Test(timeout = 50000)
    public void testReplaceValue_NullPointerException() throws IOException {
        try (ChronicleMap c = newIntString()) {
            c.replace(null, "A", "whatever");
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * putIfAbsent(x, null) throws NPE
     */
    @Test(timeout = 50000)
    public void testPutIfAbsent2_NullPointerException() throws IOException {
        try (ChronicleMap c = newIntString()) {
            c.putIfAbsent(notPresent, null);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * replace(x, null) throws NPE
     */
    @Test(timeout = 50000)
    public void testReplace2_NullPointerException() throws IOException {
        try (ChronicleMap c = newIntString()) {
            c.replace(notPresent, null);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * replace(x, null, y) throws NPE
     */
    @Test(timeout = 50000)
    public void testReplaceValue2_NullPointerException() throws IOException {
        try (ChronicleMap c = newIntString()) {
            c.replace(notPresent, null, "A");
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * replace(x, y, null) throws NPE
     */
    @Test(timeout = 50000)
    public void testReplaceValue3_NullPointerException() throws IOException {
        try (ChronicleMap c = newIntString()) {
            c.replace(notPresent, "A", null);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * remove(null) throws NPE
     */
    @Test(timeout = 50000)
    public void testRemove1_NullPointerException() throws IOException {
        try (ChronicleMap c = newStrStrMap()) {
            c.put("sadsdf", "asdads");
            c.remove(null);
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * remove(null, x) throws NPE
     */
    @Test(timeout = 50000)
    public void testRemove2_NullPointerException
    () throws IOException {
        try (ChronicleMap c = newStrStrMap()) {
            c.put("sadsdf", "asdads");
            c.remove(null, "whatever");
            shouldThrow();
        } catch (NullPointerException success) {
        }
    }

    /**
     * remove(x, null) returns false
     */
    @Test(timeout = 50000)
    public void testRemove3() throws IOException {

        try (ChronicleMap c = newStrStrMap()) {
            c.put("sadsdf", "asdads");
            assertFalse(c.remove("sadsdf", null));
        }
    }

    // classes for testing Comparable fallbacks
    static class BI implements Comparable<BI> {
        private final int value;

        BI(int value) {
            this.value = value;
        }

        public int compareTo(BI other) {
            return Integer.compare(value, other.value);
        }

        public boolean equals(Object x) {
            return (x instanceof BI) && ((BI) x).value == value;
        }

        public int hashCode() {
            return 42;
        }
    }

    static class CI extends BI {
        CI(int value) {
            super(value);
        }
    }

    static class DI extends BI {
        DI(int value) {
            super(value);
        }
    }

    static class BS implements Comparable<BS> {
        private final String value;

        BS(String value) {
            this.value = value;
        }

        public int compareTo(BS other) {
            return value.compareTo(other.value);
        }

        public boolean equals(Object x) {
            return (x instanceof BS) && value.equals(((BS) x).value);
        }

        public int hashCode() {
            return 42;
        }
    }

    static class LexicographicList<E extends Comparable<E>> extends ArrayList<E>
            implements Comparable<LexicographicList<E>> {
        private static final long serialVersionUID = 0;
        static long total;
        static long n;

        LexicographicList(Collection<E> c) {
            super(c);
        }

        LexicographicList(E e) {
            super(Collections.singleton(e));
        }

        public int compareTo(LexicographicList<E> other) {
            long start = System.currentTimeMillis();
            int common = Math.min(size(), other.size());
            int r = 0;
            for (int i = 0; i < common; i++) {
                if ((r = get(i).compareTo(other.get(i))) != 0)
                    break;
            }
            if (r == 0)
                r = Integer.compare(size(), other.size());
            total += System.currentTimeMillis() - start;
            n++;
            return r;
        }
    }

}

