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

package net.openhft.chronicle.engine.api;

import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.engine.Chassis;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.network.VanillaSessionDetails;
import net.openhft.chronicle.network.api.session.SessionDetails;
import net.openhft.chronicle.network.api.session.SessionProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SessionProviderTest {

    private ThreadDump threadDump;

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    @After
    public void checkThreadDump() {
        threadDump.assertNoNewThreads();
    }
    @Test
    public void testAcquireSessionProvider() {
        Chassis.resetChassis();
        SessionProvider sessionProvider = Chassis.getAsset("").getView(SessionProvider.class);
        VanillaSessionDetails sessionDetails0 = new VanillaSessionDetails();

        MyInfo info0 = new MyInfo();
        sessionDetails0.userId("userId");
        sessionDetails0.domain("TEST-DOMAIN");
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