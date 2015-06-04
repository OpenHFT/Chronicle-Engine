package net.openhft.chronicle.engine2;

import net.openhft.chronicle.engine.api.RequestContext;
import org.junit.Test;

import static net.openhft.chronicle.engine.api.RequestContext.requestContext;
import static org.junit.Assert.assertEquals;

/**
 * Created by daniel on 29/05/15.
 */
public class RequestContextTest {

    @Test
    public void testParsing() {
        String uri = "/chronicleMapString?" +
                "view=map&" +
                "keyType=java.lang.String&" +
                "valueType=string&" +
                "putReturnsNull=true&" +
                "removeReturnsNull=false&" +
                "bootstrap=true";
        System.out.println(uri);
        RequestContext rc = requestContext(uri);
        assertEquals("RequestContext{" +
                "pathName='', " +
                "name='chronicleMapString', " +
                "viewType=interface net.openhft.chronicle.engine2.api.map.MapView, " +
                "type=class java.lang.String, " +
                "type2=class java.lang.String, " +
                "basePath='null', " +
                "wireType=" + rc.wireType() + ", " +
                "putReturnsNull=true, " +
                "removeReturnsNull=false, " +
                "bootstrap=true}", rc.toString());
        assertEquals(Boolean.TRUE, rc.putReturnsNull());
        assertEquals(Boolean.FALSE, rc.removeReturnsNull());
        assertEquals(Boolean.TRUE, rc.bootstrap());
    }
}
