package net.openhft.chronicle.engine.api.pubsub;

import net.openhft.chronicle.engine.api.EngineReplication;
import org.jetbrains.annotations.NotNull;

/**
 * Created by Rob Austin
 */
public interface Replication {

    /**
     * removes or puts the entry into the map
     */
    void applyReplication(@NotNull EngineReplication.ReplicationEntry replicatedEntry);
}
