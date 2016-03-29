/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.server.internal.QueueSourceReplicationHandler.QueueReplicationEvent;
import net.openhft.chronicle.engine.tree.ChronicleQueueView;
import net.openhft.chronicle.engine.tree.QueueView;
import net.openhft.chronicle.network.cluster.AbstractSubHandler;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StreamCorruptedException;

/**
 * Created by Rob Austin
 */
public class QueueSyncReplicationHandler extends AbstractSubHandler<EngineWireNetworkContext>
        implements Demarshallable, WriteMarshallable {

    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(QueueReplicationEvent.class);
    }

    private static final Logger LOG = LoggerFactory.getLogger(QueueSyncReplicationHandler.class);

    private Asset rootAsset;
    private ExcerptAppender appender;

    private ChronicleQueueView chronicleQueueView;


    @UsedViaReflection
    private QueueSyncReplicationHandler(WireIn wire) {
    }

    public QueueSyncReplicationHandler() {
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
    }


    @Override
    public void onInitialize(@NotNull WireOut outWire) {
        rootAsset = nc().rootAsset();
        final Asset asset = rootAsset.acquireAsset(csp());
        chronicleQueueView = (ChronicleQueueView) asset.acquireView(QueueView
                .class);
        final ChronicleQueue chronicleQueue = chronicleQueueView.chronicleQueue();
        appender = chronicleQueue.createAppender();
        assert appender != null;
    }

    @Override
    public void processData(@NotNull WireIn inWire, @NotNull WireOut outWire) {

        final StringBuilder eventName = Wires.acquireStringBuilder();
        final ValueIn valueIn = inWire.readEventName(eventName);

        // receives replication events
        if ("replicationEvent".contentEquals(eventName)) {
            final QueueReplicationEvent replicationEvent = valueIn.typedMarshallable();

            if (replicationEvent == null)
                return;

            try {
                appender.writeBytes(replicationEvent.index(),
                        replicationEvent.payload().bytesForRead());
            } catch (StreamCorruptedException e) {
                LOG.error("", e);
            }

        } else {
            LOG.error("", new IllegalStateException("unsupported eventName=" + eventName));
        }

    }


}