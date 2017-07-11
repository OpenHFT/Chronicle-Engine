package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.EngineReplication;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class ChronicleMapV3EngineReplication implements EngineReplication {
    @Override
    public void applyReplication(@NotNull final ReplicationEntry replicatedEntry) {
        
    }

    @Override
    public byte identifier() {
        return 0;
    }

    @Nullable
    @Override
    public ModificationIterator acquireModificationIterator(final byte remoteIdentifier) {
        return null;
    }

    @Override
    public long lastModificationTime(final byte remoteIdentifier) {
        return 0;
    }

    @Override
    public void setLastModificationTime(final byte identifier, final long timestamp) {

    }
}
