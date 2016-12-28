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

package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static net.openhft.chronicle.engine.api.tree.RequestContext.requestContext;
import static org.junit.Assert.assertEquals;

/**
 * Created by daniel on 29/05/15.
 */
public class RequestContextTest {

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
    public void testParsing() {
        @NotNull String uri = "/chronicleMapString?" +
                "view=map&" +
                "keyType=java.lang.Integer&" +
                "valueType=string&" +
                "putReturnsNull=true&" +
                "removeReturnsNull=false&" +
                "bootstrap=true";
//        System.out.println(uri);
        @NotNull RequestContext rc = requestContext(uri);
        assertEquals("/chronicleMapString?" +
                "view=Map&" +
                "keyType=int&" +
                "putReturnsNull=true&" +
                "removeReturnsNull=true&" +
                "bootstrap=true&" +
                "throttlePeriodMs=0", rc.toUri());
        assertEquals("RequestContext{pathName='',\n" +
                "name='chronicleMapString',\n" +
                "viewType=interface net.openhft.chronicle.engine.api.map.MapView,\n" +
                "type=class java.lang.Integer,\n" +
                "type2=class java.lang.String,\n" +
                "basePath='null',\n" +
                "wireType=TEXT,\n" +
                "putReturnsNull=true,\n" +
                "removeReturnsNull=false,\n" +
                "bootstrap=true,\n" +
                "averageValueSize=0.0,\n" +
                "entries=0,\n" +
                "recurse=null,\n" +
                "endSubscriptionAfterBootstrap=null,\n" +
                "throttlePeriodMs=0,\n" +
                "dontPersist=false}", rc.toString().replaceAll(", ", ",\n"));
        assertEquals(Boolean.TRUE, rc.putReturnsNull());
        assertEquals(Boolean.FALSE, rc.removeReturnsNull());
        assertEquals(Boolean.TRUE, rc.bootstrap());
    }

    @Test
    public void parseDirectory() {
        @NotNull String uri = "/grandparent/parent/child/";
        @NotNull RequestContext rc = requestContext(uri);
        assertEquals("child", rc.name());
    }

    @Test
    public void parseEmptyString() {
        @NotNull String uri = "";
        @NotNull RequestContext rc = requestContext(uri);
        assertEquals("", rc.name());
    }
}
