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

package net.openhft.chronicle.engine.api;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.engine.api.pubsub.Replication;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

/**
 * @author Rob Austin.
 */
public interface EngineReplication extends Replication {


    /**
     * Provides the unique Identifier associated with this instance. <p> An identifier is used
     * to determine which replicating node made the change. <p> If two nodes update their map at the
     * same time with different values, we have to deterministically resolve which update wins,
     * because of eventual consistency both nodes should end up locally holding the same data.
     * Although it is rare two remote nodes could receive an update to their maps at exactly the
     * same time for the same key, we have to handle this edge case, its therefore important not to
     * rely on timestamps alone to reconcile the updates. Typically the update with the newest
     * timestamp should win,  but in this example both timestamps are the same, and the decision
     * made to one node should be identical to the decision made to the other. We resolve this
     * simple dilemma by using a node identifier, each node will have a unique identifier, the
     * update from the node with the smallest identifier wins.
     *
     * @return the unique Identifier associated with this map instance
     */
    byte identifier();

    default void forEach(byte remoteIdentifier, @NotNull Consumer<ReplicationEntry> consumer) throws
            InterruptedException {
        acquireModificationIterator(remoteIdentifier).forEach(consumer);
    }

    /**
     * Gets (if it does not exist, creates) an instance of ModificationIterator associated with a
     * remote node, this weak associated is bound using the {@code identifier}.
     *
     * @param remoteIdentifier the identifier of the remote node
     * @return the ModificationIterator dedicated for replication to the remote node with the given
     * identifier
     * @see #identifier()
     */
    ModificationIterator acquireModificationIterator(byte remoteIdentifier);

    /**
     * Returns the timestamp of the last change from the specified remote node, already replicated
     * to this host.  <p>Used in conjunction with replication, to back fill data from a remote
     * node. This node may have missed updates while it was not been running or connected via TCP.
     *
     * @param remoteIdentifier the identifier of the remote node to check last replicated update
     *                         time from
     * @return a timestamp of the last modification to an entry, or 0 if there are no entries.
     * @see #identifier()
     */
    long lastModificationTime(byte remoteIdentifier);

    void setLastModificationTime(byte identifier, long timestamp);

    /**
     * notifies when there is a changed to the modification iterator
     */
    @FunctionalInterface
    interface ModificationNotifier {
        ModificationNotifier NOP = () -> {
        };

        /**
         * called when ever there is a change applied to the modification iterator
         */
        void onChange();
    }

    /**
     * Holds a record of which entries have modification. Each remote map supported will require a
     * corresponding ModificationIterator instance
     */
    interface ModificationIterator {

        void forEach(@NotNull Consumer<ReplicationEntry> consumer);

        boolean hasNext();
        /**
         * Dirties all entries with a modification time equal to {@code fromTimeStamp} or newer. It
         * means all these entries will be considered as "new" by this ModificationIterator and
         * iterated once again no matter if they have already been.  <p>This functionality is used
         * to publish recently modified entries to a new remote node as it connects.
         *
         * @param fromTimeStamp the timestamp from which all entries should be dirty
         */
        void dirtyEntries(long fromTimeStamp) throws InterruptedException;

        /**
         * the {@code modificationNotifier} is called when ever there is a change applied to the
         * modification iterator
         *
         * @param modificationNotifier gets notified when a change occurs
         */
        void setModificationNotifier(@NotNull final ModificationNotifier modificationNotifier);
    }

    /**
     * Implemented typically by a replicator, This interface provides the event, which will get
     * called whenever a put() or remove() has occurred to the map
     */
    @FunctionalInterface
    interface EntryCallback {

        /**
         * Called whenever a put() or remove() has occurred to a replicating map.
         */
        boolean onEntry(@NotNull ReplicationEntry entry);
    }

    interface ReplicationEntry extends Marshallable {
        BytesStore key();

        BytesStore value();

        long timestamp();

        byte identifier();

        boolean isDeleted();

        long bootStrapTimeStamp();

        void key(BytesStore key);

        void value(BytesStore key);

        void timestamp(long timestamp);

        void identifier(byte identifier);

        void isDeleted(boolean isDeleted);

        void bootStrapTimeStamp(long bootStrapTimeStamp);


        @Override
        default void readMarshallable(@NotNull final WireIn wire) throws IllegalStateException {
            key(wire.read(() -> "key").bytesStore());
            value(wire.read(() -> "value").bytesStore());
            timestamp(wire.read(() -> "timestamp").int64());
            identifier(wire.read(() -> "identifier").int8());
            isDeleted(wire.read(() -> "isDeleted").bool());
            bootStrapTimeStamp(wire.read(() -> "bootStrapTimeStamp").int64());
        }


        @Override
        default void writeMarshallable(@NotNull final WireOut wire) {
            wire.write(() -> "key").bytes(key());
            wire.write(() -> "value").bytes(value());
            wire.write(() -> "timestamp").int64(timestamp());
            wire.write(() -> "identifier").int8(identifier());
            wire.write(() -> "isDeleted").bool(isDeleted());
            wire.write(() -> "bootStrapTimeStamp").int64(bootStrapTimeStamp());
        }


    }
}
