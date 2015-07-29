package net.openhft.chronicle.engine.api.pubsub;

import net.openhft.chronicle.engine.api.EngineReplication.ModificationIterator;
import net.openhft.chronicle.engine.api.EngineReplication.ReplicationEntry;
import net.openhft.chronicle.engine.api.tree.View;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Created by Rob Austin
 */
public interface Replication extends View {

    /**
     * removes or puts the entry into the map
     */
    void applyReplication(@NotNull ReplicationEntry replicatedEntry);

    @Nullable
    ModificationIterator acquireModificationIterator(byte id);

    long lastModificationTime(byte id);
}
