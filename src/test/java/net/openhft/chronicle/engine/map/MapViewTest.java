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

import net.openhft.chronicle.engine.Chassis;
import net.openhft.chronicle.engine.api.set.KeySetView;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by peter.lawrey on 11/06/2015.
 */
public class MapViewTest {
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
}
