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

package net.openhft.chronicle.engine.api.query;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.wire.Marshallable;
import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
public interface IndexQueueView<S extends Subscriber<IndexedValue<V>>, V extends Marshallable>
        extends Closeable {

    void registerSubscriber(@NotNull S sub,
                            @NotNull IndexQuery<V> vanillaIndexQuery);

    void unregisterSubscriber(@NotNull S listener);
}
