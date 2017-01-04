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

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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
    public void testRpc() throws IOException {

        YamlLogging.setAll(false);
        assetTree = new VanillaAssetTree(1)
                .forRemoteAccess("192.168.1.76:8088", WIRE_TYPE);

        @NotNull MapView<String, String> map = assetTree.acquireMap("/test", String.class, String.class);

        for (int i = 0; i < 9999; i++) {
            Jvm.pause(500);
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
                @Nullable String hello = map.get("hello");
                System.out.println(i + " " + hello);
            } catch (IORuntimeException e) {
                System.out.println(e.getMessage());
            }

            Jvm.pause(500);
        }
    }

    /**
     * clear removes all pairs
     */
    @Ignore("Long running")
    @Test
    public void testSub() throws IOException {

        YamlLogging.showClientWrites(true);
        YamlLogging.showClientReads(true);
        assetTree = (new VanillaAssetTree(1)).forRemoteAccess("192.168.1.76:8088", WIRE_TYPE);

        @NotNull MapView<String, String> map = assetTree.acquireMap("/test", String.class, String.class);
        @NotNull MapView<String, String> map2 = assetTree.acquireMap("/test2", String.class, String
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

            Jvm.pause(500);
        }
    }

    @After
    public void preAfter() {
        assetTree.close();
    }
}

