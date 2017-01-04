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
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.tree.*;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;

import static org.easymock.EasyMock.*;

/**
 * Created by peter on 11/06/15.
 */
public class AssetSubscriptionsTest {

    private ThreadDump threadDump;

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    @After
    public void checkThreadDump() {
        threadDump.assertNoNewThreads();
    }

    @After
    public void afterMethod() {

    }

    @Ignore("todo fix")
    @Test
    public void testSubscriptionsAtEachLevel() throws InvalidSubscriberException {
        // start at the top.
        @NotNull AssetTree tree = new VanillaAssetTree().forTesting();
        Subscriber<TopologicalEvent> rootTopoSubscriber = createMock("sub", Subscriber.class);
        Subscriber<MapEvent> rootMapSubscriber = createMock(Subscriber.class);
        Subscriber<String> rootNameSubscriber = createMock(Subscriber.class);
        TopicSubscriber<String, String> rootTopicSubscriber = createMock(TopicSubscriber.class);

        // the root asset
        rootTopoSubscriber.onMessage(ExistingAssetEvent.of(null, "", Collections.emptySet()));
        // the one added
        rootTopoSubscriber.onMessage(ExistingAssetEvent.of("/", "queue", Collections.emptySet()));
        rootTopoSubscriber.onMessage(AddedAssetEvent.of("/", "one", Collections.emptySet()));
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
        rootTopoSubscriber1.onMessage(ExistingAssetEvent.of("/", "one", Collections.emptySet()));

        replay(rootTopoSubscriber1, rootMapSubscriber1, rootNameSubscriber1, rootTopicSubscriber1);

        tree.registerSubscriber("one", TopologicalEvent.class, rootTopoSubscriber1);
        tree.registerSubscriber("one", MapEvent.class, rootMapSubscriber1);
        tree.registerSubscriber("one", String.class, rootNameSubscriber1);
        tree.registerTopicSubscriber("one", String.class, String.class, rootTopicSubscriber1);

        verify(rootTopoSubscriber, rootMapSubscriber, rootNameSubscriber, rootTopicSubscriber);
        verify(rootTopoSubscriber1, rootMapSubscriber1, rootNameSubscriber1, rootTopicSubscriber1);

        Subscriber<TopologicalEvent> rootTopoSubscriber0 = createMock("sub0", Subscriber.class);
        rootTopoSubscriber0.onMessage(ExistingAssetEvent.of(null, "", Collections.emptySet()));
        rootTopoSubscriber0.onMessage(ExistingAssetEvent.of("/", "queue", Collections.emptySet()));
        rootTopoSubscriber0.onMessage(ExistingAssetEvent.of("/", "one", Collections.emptySet()));
        replay(rootTopoSubscriber0);
        tree.registerSubscriber("", TopologicalEvent.class, rootTopoSubscriber0);
        verify(rootTopoSubscriber0);

        // test removing an asset
        reset(rootTopoSubscriber, rootTopoSubscriber0, rootTopoSubscriber1);
        rootTopoSubscriber.onMessage(RemovedAssetEvent.of("/", "one", Collections.emptySet()));
        rootTopoSubscriber0.onMessage(RemovedAssetEvent.of("/", "one", Collections.emptySet()));
        rootTopoSubscriber1.onMessage(RemovedAssetEvent.of("/", "one", Collections.emptySet()));

        replay(rootTopoSubscriber, rootTopoSubscriber0, rootTopoSubscriber1);

        tree.getAsset("").removeChild("one");
        verify(rootTopoSubscriber, rootTopoSubscriber0, rootTopoSubscriber1);

    }
}
