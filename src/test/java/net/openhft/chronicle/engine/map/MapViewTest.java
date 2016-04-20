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

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.engine.Chassis;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.query.Subscription;
import net.openhft.chronicle.engine.api.set.KeySetView;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by peter.lawrey on 11/06/2015.
 */
public class MapViewTest {

    private ThreadDump threadDump;

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    @After
    public void checkThreadDump() {
        threadDump.assertNoNewThreads();
    }

    @Before
    public void setUp() {
        Chassis.resetChassis();
    }

    @Test
    public void keySet() {
        Map<String, String> map = Chassis.acquireMap("test", String.class, String.class);
        map.put("a", "one");
        map.put("b", "two");
        map.put("c", "three");
        Set<String> keys = map.keySet();
        assertTrue(KeySetView.class.isInstance(keys));
        assertEquals("[a, b, c]", keys.toString());
        assertEquals(new HashSet<>(keys), keys);
        int hc = keys.hashCode();
        assertEquals(new HashSet<>(keys).hashCode(), hc);
    }

    @Test
    public void testRemoteAccess() throws IOException {
        Chassis.resetChassis();

        MapView<String, UserInfo> userMap = Chassis.acquireMap("users", String.class, UserInfo.class);
        userMap.put("userid", new UserInfo("User's Name"));

        userMap.registerSubscriber(System.out::println);

        // obtain just the fullName.
        String fullName = userMap.applyToKey("userid", ui -> ui.fullName);

        // increment a counter.
        userMap.asyncUpdateKey("userid", ui -> {
            ui.usageCounter++;
            return ui;
        });

        // increment a counter and return the userid
        int count = userMap.syncUpdateKey("userid",
                ui -> {
                    ui.usageCounter++;
                    return ui;
                },
                ui -> ui.usageCounter);

        Map<Integer, List<UserInfo>> collect = userMap.entrySet().query()
                .filter(e -> e.getKey().matches("u*d"))
                .map(e -> e.getValue())
                .collect(Collectors.groupingBy(u -> u.usageCounter));

// print userid which have a usageCounter > 10 each time it is incremented.
        Subscription subscription = userMap.entrySet().query()
                .filter(e -> e.getValue().usageCounter > 10)
                .map(e -> e.getKey())
                .subscribe(System.out::println);
        subscription.cancel();

        Function<UserInfo, String> fullNameFunc = (Function<UserInfo, String> & Serializable) ui -> ui.fullName;
//String fullName = userInfo.applyToKey("userid", fullNameFunc);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(fullNameFunc);
        oos.close();
        System.out.println("fullNameFunc.size=" + baos.toByteArray().length);
    }
}

class UserInfo {
    final String fullName;
    int usageCounter;

    UserInfo(String fullName) {
        this.fullName = fullName;
    }
}