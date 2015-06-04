package net.openhft.chronicle.engine2.api;

import net.openhft.chronicle.engine2.Chassis;
import net.openhft.chronicle.engine2.session.VanillaSessionDetails;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SessionProviderTest {
    @Test
    public void testAcquireSessionProvider() {
        SessionProvider sessionProvider = Chassis.getAsset("").getView(SessionProvider.class);
        sessionProvider.set(new VanillaSessionDetails("userId"));

        // get me a node in the graph
        Asset asset = Chassis.acquireAsset("test", Void.class, null, null);
        SessionDetails sessionDetails = asset.root().getView(SessionProvider.class).get();
        assertEquals("userId", sessionDetails.userId());
    }
}