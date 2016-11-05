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
 * Based on a filter, Provides a snapshot ( some times referred to as an image ) of all the *
 * latest values and then subsequent updates.   The snapshot can be considered to work much like *
 * a map does, where the latest values are returned based on the keys. You can specify the index *
 * from   where you want the data, so you will initially get the snapshot and then all subsequent
 * changes.  Even if the server is restarted, The snapshot is Guaranteed to provide at least all
 * the data for the * current roll cycle , the snapshot will always contain only the fields that are
 * known not to then subsequently change up to the current end of the   queue. If the server has not
 * been restarted then be snapshot is built up using the changes   since the start of the cycle in
 * which it   was started.  If the server is restarted and you ask for an index that before the
 * current roll  cycle, the response then becomes the same as a filtered response from a queue. If
 * you on the * other hand as for an index that's mid current cycle you will get a snapshot that is
 * made up of * all the fields that are know not to subsequently change until the current end of the
 * queue. Then * followed by all the changes from that point ( like a queue does), this way  once
 * you have read * the queue to the end, your data model will always be fully updated for all your
 * keys, regardless   of where in the current roll cycle you decide to replay the data from. If you
 * specify the index   of 0 then the data will be replayed from the end of the queue and the
 * snapshot will contain all   the latest values for all the keys.   So In summary : The
 * IndexQueueView is ideal if all your entries change each roll cycle,   IndexQueueView is not able
 * to give you a snapshot where the entries have not * changed since the last roll cycle, such as
 * yesterday.
 *
 * @author Rob Austin.
 */
public interface IndexQueueView<S extends Subscriber<IndexedValue<V>>, V extends Marshallable>
        extends Closeable {

    void registerSubscriber(@NotNull S sub,
                            @NotNull IndexQuery<V> vanillaIndexQuery);

    void unregisterSubscriber(@NotNull S listener);


}
