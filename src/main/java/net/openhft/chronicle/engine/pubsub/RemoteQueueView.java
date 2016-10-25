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

package net.openhft.chronicle.engine.pubsub;

import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.tree.ChronicleQueueView.LocalExcept;
import net.openhft.chronicle.engine.tree.QueueView;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static net.openhft.chronicle.engine.server.internal.TopicPublisherHandler.EventId.*;

/**
 * @author Rob Austin.
 */
public class RemoteQueueView<T, M> extends RemoteTopicPublisher<T, M> implements QueueView<T, M> {

    final ThreadLocal<LocalExcept<T, M>> threadLocal = ThreadLocal.withInitial(LocalExcept::new);
    private final Asset asset;

    public RemoteQueueView(@NotNull RequestContext requestContext, @NotNull Asset asset) {
        super(requestContext, asset, "QueueView");
        this.asset = asset;
    }

    @Override
    public Excerpt<T, M> getExcerpt(long index) {
        //noinspection unchecked
        return proxyReturnWireTypedObject(getNextAtIndex, threadLocal.get(), LocalExcept.class, index);
    }

    @Override
    public Excerpt<T, M> getExcerpt(T topic) {
        //noinspection unchecked
        return proxyReturnWireTypedObject(getNextAtTopic, threadLocal.get(), LocalExcept.class, topic);
    }

    @Override
    public long publishAndIndex(@NotNull T topic, @NotNull M message) {
        return proxyReturnLongWithArgs(publishAndIndex, topic, message);
    }

    @Override
    public Asset asset() {
        return asset;
    }

    @Nullable
    @Override
    public Object underlying() {
        throw new UnsupportedOperationException("todo");
    }
}
