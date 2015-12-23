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

package net.openhft.chronicle.engine.eg;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;

import java.util.Map;

/**
 * Created by peter on 17/08/15.
 */
/*
Run ServerMain first

prints
Funny: hihi
Funny: haha
Funny: hehe
Funny: hoho
Music: Queen
Music: Police
Music: Gun's N Roses
Music: U2
InsertedEvent{assetName='/test-map', key=MUSIC, value=U2}
InsertedEvent{assetName='/test-map', key=DOGS, value=St Bernard}
InsertedEvent{assetName='/test-map', key=FUNNY, value=hoho}
InsertedEvent{assetName='/test-map', key=LEADERSHIP, value=Queen}
UpdatedEvent{assetName='/test-map', key=LEADERSHIP, oldValue=Queen, value=President}
UpdatedEvent{assetName='/test-map', key=LEADERSHIP, oldValue=President, value=Prime Minister}
RemovedEvent{assetName='/test-map', key=LEADERSHIP, oldValue=Prime Minister}
UpdatedEvent{assetName='/test-map', key=DOGS, oldValue=St Bernard, value=Poodle}
 */
public class ClientOneMain {
    public static void main(String[] args) {
        YamlLogging.setAll(true);
        VanillaAssetTree assetTree = new VanillaAssetTree().forRemoteAccess("localhost:9090",
                WireType.TEXT, t -> t.printStackTrace());

        Map<String, String> map = assetTree.acquireMap("/test-map", String.class, String.class);
        map.put("FUNNY", "hihi");
        map.put("LEADERSHIP", "Queen");
        map.put("MUSIC", "Queen");
        map.put("DOGS", "Retriever");

        assetTree.registerSubscriber("/test-map/FUNNY", String.class, s -> System.out.println("Funny: " + s));

        map.put("FUNNY", "haha");
        map.put("FUNNY", "hehe");
        map.put("FUNNY", "hoho");
        map.put("DOGS", "St Bernard");

        assetTree.registerSubscriber("/test-map/MUSIC", String.class, s -> System.out.println("Music: " + s));

        map.put("MUSIC", "Police");
        map.put("MUSIC", "Gun's N Roses");
        map.put("MUSIC", "U2");

        assetTree.registerSubscriber("/test-map", MapEvent.class, System.out::println);

        map.put("LEADERSHIP", "President");
        map.put("LEADERSHIP", "Prime Minister");
        map.remove("LEADERSHIP");
        map.put("DOGS", "Poodle");

        Jvm.pause(1000);
    }
}
