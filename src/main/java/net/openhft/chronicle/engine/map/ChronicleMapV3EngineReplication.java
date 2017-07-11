package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class ChronicleMapV3EngineReplication implements EngineReplication {
    public ChronicleMapV3EngineReplication(final RequestContext requestContext, @NotNull final Asset asset) {
        // TODO mark.price
        System.out.printf(Thread.currentThread().getName() + "|++!! Replication, name: %s (%s)%n",
                asset.name(), asset.fullName());
    }

    @Override
    public void applyReplication(@NotNull final ReplicationEntry replicatedEntry) {
        // TODO mark.price
        System.out.printf(Thread.currentThread().getName() + "|++!! %s%n",
                replicatedEntry);
    }

    @Override
    public byte identifier() {
        return 1;
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
