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

package net.openhft.chronicle.engine.collection;

import net.openhft.chronicle.engine.api.column.ClosableIterator;
import net.openhft.chronicle.engine.api.column.Row;
import net.openhft.chronicle.engine.server.internal.ColumnViewIteratorHandler.EventId;
import net.openhft.chronicle.network.connection.AbstractStatelessClient;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.WriteValue;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

public class RemoteColumnViewRowIterator extends
        AbstractStatelessClient<CollectionWireHandler.EventId> implements ClosableIterator<Row> {

    private static final int ITTERATOR_PAGE_SIZE = 300;
    private Iterator<Row> iterator;
    private final WriteValue pageSize = valueOut -> valueOut.int32(ITTERATOR_PAGE_SIZE);
    @NotNull
    private AtomicBoolean isClosed = new AtomicBoolean();

    public RemoteColumnViewRowIterator(@NotNull TcpChannelHub hub,
                                       @NotNull String csp,
                                       long cid) {
        super(hub, cid, csp);
    }

    @Override
    public synchronized boolean hasNext() {

        if (iterator != null && iterator.hasNext())
            return true;

        iterator = nextIterator();
        return iterator.hasNext();
    }

    @Override
    public synchronized Row next() {
        if (iterator != null && iterator.hasNext())
            return iterator.next();

        iterator = nextIterator();

        if (!iterator.hasNext())
            throw new NoSuchElementException();

        return next();

    }

    private Iterator<Row> nextIterator() {
        return this.<Collection<Row>>proxyReturnWireConsumerInOut(
                EventId.next,
                CoreFields.reply,
                pageSize,
                f -> f.object(List.class)).iterator();
    }

    @Override
    public void close() {
        isClosed.set(true);
        proxyReturnVoid(EventId.close);
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            close();
        } catch (Throwable t) {
            t.printStackTrace();
        }
        super.finalize();

    }
}