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

import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.junit.rules.TestName;

import java.io.IOException;

import static net.openhft.chronicle.engine.Utils.methodName;

public class RemoteRpc extends JSR166TestCase {

    public static final net.openhft.chronicle.wire.WireType WIRE_TYPE = net.openhft.chronicle.wire.WireType.TEXT;
    @NotNull
    @Rule
    public TestName name = new TestName();
    AssetTree assetTree;

    @Before
    public void before() {
        System.out.println("\t... test " + name.getMethodName());
        methodName(name.getMethodName());
    }

    /**
     * clear removes all pairs
     */
    @Ignore("Long running")
    @Test
    public void testRpc() throws IOException, InterruptedException {

        YamlLogging.setAll(false);
        assetTree = new VanillaAssetTree(1)
                .forRemoteAccess("192.168.1.76:8088", WIRE_TYPE, x -> t.set(x));

        MapView<String, String> map = assetTree.acquireMap("/test", String.class, String.class);

        for (int i = 0; i < 9999; i++) {
            Thread.sleep(1000);
            try {
                map.put("hello", "world");
            } catch (IORuntimeException e) {
                System.out.println(e.getMessage());
                continue;
            } catch (Error e) {
                e.printStackTrace();
            }
            break;
        }

        for (int i = 0; i < 9999; i++) {
            try {
                String hello = map.get("hello");
                System.out.println(i + " " + hello);
            } catch (IORuntimeException e) {
                System.out.println(e.getMessage());
            }

            Thread.sleep(1000);
        }
    }

    /**
     * clear removes all pairs
     */
    @Ignore("Long running")
    @Test
    public void testSub() throws IOException, InterruptedException {

        YamlLogging.showClientWrites(true);
        YamlLogging.showClientReads(true);
        assetTree = (new VanillaAssetTree(1)).forRemoteAccess("192.168.1.76:8088", WIRE_TYPE, x -> t.set(x));

        MapView<String, String> map = assetTree.acquireMap("/test", String.class, String.class);
        MapView<String, String> map2 = assetTree.acquireMap("/test2", String.class, String
                .class);
        map.put("hello", "world");

        assetTree.registerSubscriber("/test", String.class, (x) -> System.out.println
                ("******************+" + x));
        assetTree.registerSubscriber("/test2", String.class, (x) -> System.out.println
                ("------------------*+" + x));

        for (int i = 0; i < 9999; i++) {
            try {
                map.put("hello", "world" + i);
                map2.put("goodbye", "world" + i);

            } catch (IORuntimeException e) {

                e.printStackTrace();
            }

            Thread.sleep(1000);
        }
    }

    @After
    public void preAfter() {
        assetTree.close();
    }
}

