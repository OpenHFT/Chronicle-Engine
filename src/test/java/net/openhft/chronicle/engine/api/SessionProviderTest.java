package net.openhft.chronicle.engine.api;

import net.openhft.chronicle.engine.Chassis;
import net.openhft.chronicle.engine.api.session.SessionProvider;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.session.VanillaSessionDetails;
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
        Asset asset = Chassis.acquireAsset("test", Void.class, null, null);
        SessionDetails sessionDetails = asset.findView(SessionProvider.class).get();
        assertEquals("userId", sessionDetails.userId());
        assertEquals(info0, sessionDetails.get(MyInfo.class));
    }

    static class MyInfo {
        // place application specific session details here.
    }
}