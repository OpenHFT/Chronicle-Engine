package net.openhft.chronicle.engine.api;

import net.openhft.chronicle.engine.Chassis;
import net.openhft.chronicle.engine.session.VanillaSessionDetails;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SessionProviderTest {
    @Ignore
    @Test
    public void testAcquireSessionProvider() {
        SessionProvider sessionProvider = Chassis.getAsset("").getView(SessionProvider.class);
        VanillaSessionDetails sessionDetails0 = new VanillaSessionDetails("userId");
        MyInfo info0 = new MyInfo();
        sessionDetails0.set(MyInfo.class, info0);
        sessionProvider.set(sessionDetails0);

        // get me a node in the graph
        Asset asset = Chassis.acquireAsset("test", Void.class, null, null);
        SessionDetails sessionDetails = asset.root().getView(SessionProvider.class).get();
        assertEquals("userId", sessionDetails.userId());
        assertEquals(info0, sessionDetails.get(MyInfo.class));
    }

    static class MyInfo {
        // place application specific session details here.
    }
}