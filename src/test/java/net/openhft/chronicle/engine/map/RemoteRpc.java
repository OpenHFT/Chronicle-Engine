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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.WireType;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.junit.rules.TestName;

import java.io.IOException;

import static net.openhft.chronicle.engine.Utils.methodName;


public class RemoteRpc extends JSR166TestCase {

    private static int s_port = 11050;
    @NotNull
    @Rule
    public TestName name = new TestName();

    @NotNull

    @Before
    public void before() {
        System.out.println("\t... test " + name.getMethodName());
        methodName(name.getMethodName());
    }

    AssetTree assetTree;

    /**
     * clear removes all pairs
     */
    @Ignore
    @Test(timeout = 50000)
    public void testPut() throws IOException {
        WireType.wire = WireType.TEXT;
        assetTree = (new VanillaAssetTree(1)).forRemoteAccess("192.168.1.64", 8088);

        MapView<String, String, String> map = assetTree.acquireMap("/test", String.class, String.class);
        map.put("hello", "world");

        for (int i = 0; i < 9999; i++) {
            String hello = map.get("hello");
            System.out.println(i + " " + hello);
            Jvm.pause(2000);
        }
    }

    @After
    public void after() throws IOException {
        assetTree.close();
    }

}

