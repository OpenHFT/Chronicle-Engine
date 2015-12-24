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

package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.tree.*;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.easymock.EasyMock.*;

/**
 * Created by peter on 11/06/15.
 */
public class AssetSubscriptionsTest {

    private static AtomicReference<Throwable> t = new AtomicReference();

    @After
    public void afterMethod() {
        final Throwable th = t.getAndSet(null);
        if (th != null) Jvm.rethrow(th);
    }

    @Test
    public void testSubscriptionsAtEachLevel() throws InvalidSubscriberException {
        // start at the top.
        AssetTree tree = new VanillaAssetTree().forTesting(x -> t.compareAndSet(null, x));
        Subscriber<TopologicalEvent> rootTopoSubscriber = createMock("sub", Subscriber.class);
        Subscriber<MapEvent> rootMapSubscriber = createMock(Subscriber.class);
        Subscriber<String> rootNameSubscriber = createMock(Subscriber.class);
        TopicSubscriber<String, String> rootTopicSubscriber = createMock(TopicSubscriber.class);

        // the root asset
        rootTopoSubscriber.onMessage(ExistingAssetEvent.of(null, ""));
        // the one added
        rootTopoSubscriber.onMessage(ExistingAssetEvent.of("/", "queue"));
        rootTopoSubscriber.onMessage(AddedAssetEvent.of("/", "one"));
        // rootMapSubscriber - none
        // rootNameSubscriber - none
        // rootTopicSubscriber - none
        replay(rootTopoSubscriber, rootMapSubscriber, rootNameSubscriber, rootTopicSubscriber);

        tree.registerSubscriber("", TopologicalEvent.class, rootTopoSubscriber);
        tree.registerSubscriber("", MapEvent.class, rootMapSubscriber);
        tree.registerSubscriber("", String.class, rootNameSubscriber);
        tree.registerTopicSubscriber("", String.class, String.class, rootTopicSubscriber);

        // test adding an asset.
        Subscriber<TopologicalEvent> rootTopoSubscriber1 = createMock("sub1", Subscriber.class);
        Subscriber<MapEvent> rootMapSubscriber1 = createMock(Subscriber.class);
        Subscriber<String> rootNameSubscriber1 = createMock(Subscriber.class);
        TopicSubscriber<String, String> rootTopicSubscriber1 = createMock(TopicSubscriber.class);

        // the root asset
//        rootTopoSubscriber1.onMessage(ExistingAssetEvent.of(null, ""));
        rootTopoSubscriber1.onMessage(ExistingAssetEvent.of("/", "one"));

        replay(rootTopoSubscriber1, rootMapSubscriber1, rootNameSubscriber1, rootTopicSubscriber1);

        tree.registerSubscriber("one", TopologicalEvent.class, rootTopoSubscriber1);
        tree.registerSubscriber("one", MapEvent.class, rootMapSubscriber1);
        tree.registerSubscriber("one", String.class, rootNameSubscriber1);
        tree.registerTopicSubscriber("one", String.class, String.class, rootTopicSubscriber1);

        verify(rootTopoSubscriber, rootMapSubscriber, rootNameSubscriber, rootTopicSubscriber);
        verify(rootTopoSubscriber1, rootMapSubscriber1, rootNameSubscriber1, rootTopicSubscriber1);

        Subscriber<TopologicalEvent> rootTopoSubscriber0 = createMock("sub0", Subscriber.class);
        rootTopoSubscriber0.onMessage(ExistingAssetEvent.of(null, ""));
        rootTopoSubscriber0.onMessage(ExistingAssetEvent.of("/", "queue"));
        rootTopoSubscriber0.onMessage(ExistingAssetEvent.of("/", "one"));
        replay(rootTopoSubscriber0);
        tree.registerSubscriber("", TopologicalEvent.class, rootTopoSubscriber0);
        verify(rootTopoSubscriber0);

        // test removing an asset
        reset(rootTopoSubscriber, rootTopoSubscriber0, rootTopoSubscriber1);
        rootTopoSubscriber.onMessage(RemovedAssetEvent.of("/", "one"));
        rootTopoSubscriber0.onMessage(RemovedAssetEvent.of("/", "one"));
        rootTopoSubscriber1.onMessage(RemovedAssetEvent.of("/", "one"));

        replay(rootTopoSubscriber, rootTopoSubscriber0, rootTopoSubscriber1);

        tree.getAsset("").removeChild("one");
        verify(rootTopoSubscriber, rootTopoSubscriber0, rootTopoSubscriber1);

    }
}
