package net.openhft.chronicle.engine2;

import net.openhft.chronicle.engine2.api.RequestContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by daniel on 29/05/15.
 */
public class RequestContextTest {
    @Test
    public void testParsing() {
        String queryString = "RequestContext{pathName='', name='chronicleMapString', parent=null, assetType=interface java.util.concurrent.ConcurrentMap, " +
                "type=class java.lang.String, type2=class java.lang.String, item=null, basePath='null', " +
                "wireType=net.openhft.chronicle.engine2.api.RequestContext$$Lambda$3/11003494@74a10858, putReturnsNull=true, " +
                "removeReturnsNull=false, bootstrap=true}";

        RequestContext rc = new RequestContext("","");
        rc = rc.queryString(queryString);
        assertEquals(true, rc.putReturnsNull());
    }
}
