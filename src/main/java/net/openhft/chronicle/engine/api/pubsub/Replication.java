package net.openhft.chronicle.engine.api.pubsub;

import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.tree.View;
import org.jetbrains.annotations.NotNull;

/**
 * Created by Rob Austin
 */
public interface Replication extends View {

    /**
     * removes or puts the entry into the map
     */
    void applyReplication(@NotNull EngineReplication.ReplicationEntry replicatedEntry);

    EngineReplication.ModificationIterator acquireModificationIterator(byte id);

    long lastModificationTime(byte id);
}
