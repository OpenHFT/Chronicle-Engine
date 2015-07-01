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

package net.openhft.chronicle.engine.api;

import net.openhft.chronicle.engine.Chassis;
import net.openhft.chronicle.engine.api.session.SessionProvider;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.network.VanillaSessionDetails;
import net.openhft.chronicle.network.api.session.SessionDetails;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SessionProviderTest {

    @Test
    public void testAcquireSessionProvider() {
        Chassis.resetChassis();
        SessionProvider sessionProvider = Chassis.getAsset("").getView(SessionProvider.class);
        VanillaSessionDetails sessionDetails0 = new VanillaSessionDetails();

        MyInfo info0 = new MyInfo();
        sessionDetails0.setUserId("userId");
        sessionDetails0.set(MyInfo.class, info0);
        sessionProvider.set(sessionDetails0);

        // get me a node in the graph
        Asset asset = Chassis.acquireAsset("test");
        SessionDetails sessionDetails = asset.findView(SessionProvider.class).get();
        assertEquals("userId", sessionDetails.userId());
        assertEquals(info0, sessionDetails.get(MyInfo.class));
    }

    static class MyInfo {
        // place application specific session details here.
    }
}