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

package net.openhft.chronicle.engine.pubsub;

import net.openhft.chronicle.engine.api.pubsub.Reference;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.tree.ChronicleQueueView;
import net.openhft.chronicle.queue.ChronicleQueue;
import org.jetbrains.annotations.Nullable;


public class QueueReference<T, M> implements Reference<M> {

    private final Class<M> eClass;
    private final ChronicleQueueView<T, M> chronicleQueue;

    public QueueReference(RequestContext requestContext, Asset asset, ChronicleQueue chronicleQueue) {
        this.eClass = requestContext.type();
        this.chronicleQueue = (ChronicleQueueView<T, M>) chronicleQueue;
    }

    @Override
    public long set(M event) {
        return chronicleQueue.set(event);
    }

    @Nullable
    @Override
    public M get() {
        return chronicleQueue.get();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void registerSubscriber(boolean bootstrap, Subscriber<M> subscriber) throws AssetNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unregisterSubscriber(Subscriber subscriber) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int subscriberCount() {
        throw new UnsupportedOperationException();

    }

    @Override
    public Class getType() {
        return eClass;
    }
}

