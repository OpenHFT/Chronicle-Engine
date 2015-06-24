/*
 * Copyright 2015 Higher Frequency Trading
 *
 *  http://www.higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.ChronicleHashBuilder;
import net.openhft.chronicle.hash.replication.TimeProvider;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.WriteContext;
import net.openhft.lang.collection.ATSDirectBitSet;
import net.openhft.lang.collection.DirectBitSet;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.model.constraints.MaxSize;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static net.openhft.chronicle.engine.map.ChronicleMapBackedEngineReplication.Value.*;

public class ChronicleMapBackedEngineReplication<Store> implements EngineReplication {

    public static final int RESERVED_MOD_ITER = 8;
    public static final int MAX_MODIFICATION_ITERATORS = 127 + RESERVED_MOD_ITER;
    // a long word serve 64 bits
    public static final int DIRTY_WORD_COUNT = (MAX_MODIFICATION_ITERATORS + 63) / 64;

    private static int idToInt(byte identifier) {
        // if we consider > 127 ids, we should treat ids positively
        return identifier & 0xFF;
    }

    interface ChangeApplier<Store> {
        void applyChange(Store store, ReplicationEntry replicationEntry);
    }

    interface GetValue<Store> {
        Bytes getValue(Store store, Bytes key);
    }

    interface ChronicleHashConfigurer {
        <C extends ChronicleHash> C configure(ChronicleHashBuilder<?, C, ?> builder);
    }

    interface Value {
        boolean getDeleted();
        void setDeleted(boolean deleted);

        long getTimestamp();
        void setTimestamp(long timestamp);

        byte getIdentifier();
        void setIdentifier(byte identifier);

        long getDirtyWord(@MaxSize(DIRTY_WORD_COUNT) int index);
        void setDirtyWord(@MaxSize(DIRTY_WORD_COUNT) int index, long word);

        static void dropChange(Value value) {
            for (int i = 0; i < DIRTY_WORD_COUNT; i++) {
                value.setDirtyWord(i, 0);
            }
        }

        static void raiseChange(Value value) {
            for (int i = 0; i < DIRTY_WORD_COUNT; i++) {
                value.setDirtyWord(i, ~0L);
            }
        }

        static void clearChange(Value value, int identifier) {
            int index = identifier / 64;
            long bit = 1L << (identifier % 64);
            value.setDirtyWord(index, value.getDirtyWord(index) ^ bit);
        }

        static void setChange(Value value, int identifier) {
            int index = identifier / 64;
            long bit = 1L << (identifier % 64);
            value.setDirtyWord(index, value.getDirtyWord(index) | bit);
        }

        static boolean isChanged(Value value, int identifier) {
            int index = identifier / 64;
            long bit = 1L << (identifier % 64);
            return (value.getDirtyWord(index) & bit) != 0L;
        }
    }

    private static ATSDirectBitSet createModIterBitSet() {
        return new ATSDirectBitSet(new DirectStore(null, DIRTY_WORD_COUNT * 8, true).bytes());
    }

    static ThreadLocal<Value> threadLocalValue =
            ThreadLocal.withInitial(() -> DataValueClasses.newDirectReference(Value.class));

    private ChronicleMap<Bytes, Value> map;
    private BytesStore modIterState;
    private final byte identifier;
    private final Store store;
    private final ChangeApplier<Store> changeApplier;
    private final GetValue<Store> getValue;
    private final AtomicReferenceArray<ChronicleMapBackedModificationIterator> modificationIterators =
            new AtomicReferenceArray<>(127 + RESERVED_MOD_ITER);
    private final DirectBitSet modificationIteratorsRequiringSettingBootstrapTimestamp =
            createModIterBitSet();
    private final DirectBitSet modIterSet = createModIterBitSet();

    private final TimeProvider timeProvider;


    public ChronicleMapBackedEngineReplication(
            byte identifier,
            Store store, ChangeApplier<Store> changeApplier, GetValue<Store> getValue,
            TimeProvider timeProvider,
            ChronicleHashConfigurer chronicleHashConfigurer, @Nullable String stateFilePrefix)
            throws IOException {
        this.identifier = identifier;
        this.store = store;
        this.changeApplier = changeApplier;
        this.getValue = getValue;
        this.timeProvider = timeProvider;
        this.map = chronicleHashConfigurer
                .configure(ChronicleMapBuilder.of(Bytes.class, Value.class));
        if (stateFilePrefix != null) {
            // TODO init modIterState
        } else {
            // TODO init modIterState
        }
    }

    @Override
    public byte identifier() {
        return identifier;
    }

    ////////////////
    // Method for working with modIterState

    private static final long NEXT_BOOTSTRAP_TS_OFFSET = 0;
    private static final long LAST_BOOTSTRAP_TS_OFFSET = NEXT_BOOTSTRAP_TS_OFFSET + 8;
    private static final long LAST_MOD_TIME_OFFSET = LAST_BOOTSTRAP_TS_OFFSET + 8;

    private static long identifierToModIterStateOffset(int identifier) {
        return identifier * 4 * 64; // 4 cache lines for each identifier
    }

    private static long nextOff(int remoteIdentifier) {
        return identifierToModIterStateOffset(remoteIdentifier) + LAST_BOOTSTRAP_TS_OFFSET;
    }

    private static long lastModOff(int remoteIdentifier) {
        return identifierToModIterStateOffset(remoteIdentifier) + LAST_MOD_TIME_OFFSET;
    }

    private void resetNextBootstrapTimestamp(int remoteIdentifier) {
        long modIterOff = identifierToModIterStateOffset(remoteIdentifier);
        long nextOff = modIterOff + NEXT_BOOTSTRAP_TS_OFFSET;
        modIterState.writeOrderedLong(nextOff, 0);
    }

    private boolean setNextBootstrapTimestamp(int remoteIdentifier, long timestamp) {
        long nextOff = nextOff(remoteIdentifier);
        return modIterState.compareAndSwapLong(nextOff, 0, timestamp);
    }

    private void resetLastBootstrapTimestamp(int remoteIdentifier) {
        long nextOff = nextOff(remoteIdentifier);
        modIterState.writeOrderedLong(nextOff, 0);
    }

    private long bootstrapTimestamp(int remoteIdentifier) {
        long modIterOff = identifierToModIterStateOffset(remoteIdentifier);
        long nextOff = modIterOff + NEXT_BOOTSTRAP_TS_OFFSET;
        long nextBootstrapTs = modIterState.readVolatileLong(nextOff);
        long lastOff = modIterOff + LAST_BOOTSTRAP_TS_OFFSET;
        long result = (nextBootstrapTs == 0) ? modIterState.readLong(lastOff) : nextBootstrapTs;
        modIterState.writeLong(lastOff, result);
        return result;
    }

    @Override
    public long lastModificationTime(byte remoteIdentifier) {
        return lastModificationTime(idToInt(remoteIdentifier));
    }

    private long lastModificationTime(int remoteIdentifier) {
        long lastModOff = lastModOff(remoteIdentifier);
        // purposely not volatile as this will impact performance,
        // and the worst that will happen is we'll end up loading more data on a bootstrap
        return modIterState.readLong(lastModOff);
    }

    @Override
    public void setLastModificationTime(byte identifier, long timestamp) {
        setLastModificationTime(idToInt(identifier), timestamp);
    }

    private void setLastModificationTime(int identifier, long timestamp) {
        long lastModOff = identifierToModIterStateOffset(identifier) + LAST_MOD_TIME_OFFSET;
        // purposely not volatile as this will impact performance,
        // and the worst that will happen is we'll end up loading more data on a bootstrap
        if (modIterState.readLong(lastModOff) < timestamp)
            modIterState.writeLong(lastModOff, timestamp);
    }



    private static boolean shouldApplyRemoteModification(
            ReplicationEntry remoteEntry, Value localValue) {
        long remoteTimestamp = remoteEntry.timestamp();
        long originTimestamp = localValue.getTimestamp();
        return remoteTimestamp > originTimestamp || (remoteTimestamp == originTimestamp &&
                remoteEntry.identifier() <= localValue.getIdentifier());
    }

    @Override
    public void applyReplication(@NotNull ReplicationEntry replicatedEntry) {
        Value value = threadLocalValue.get();
        try (WriteContext<Bytes, Value> wc = map.acquireUsingLocked(replicatedEntry.key(), value)) {
            boolean shouldApplyRemoteModification = wc.created() ||
                    shouldApplyRemoteModification(replicatedEntry, value);
            if (shouldApplyRemoteModification) {
                changeApplier.applyChange(store, replicatedEntry);
                value.setDeleted(replicatedEntry.isDeleted());
                value.setIdentifier(replicatedEntry.identifier());
                value.setTimestamp(replicatedEntry.timestamp());
                if (!wc.created()) {
                    // if we apply the remote change, shouldn't propagate stale own change
                    dropChange(value);
                }
            }
        }

    }

    @Override
    public ModificationIterator acquireModificationIterator(byte id) {
        int remoteIdentifier = idToInt(id);

        ModificationIterator modificationIterator = modificationIterators.get(remoteIdentifier);
        if (modificationIterator != null)
            return modificationIterator;

        synchronized (modificationIterators) {
            modificationIterator = modificationIterators.get(remoteIdentifier);

            if (modificationIterator != null)
                return modificationIterator;

            final ChronicleMapBackedModificationIterator newModificationIterator =
                    new ChronicleMapBackedModificationIterator(remoteIdentifier);
            modificationIteratorsRequiringSettingBootstrapTimestamp.set(remoteIdentifier);
            resetNextBootstrapTimestamp(remoteIdentifier);
            // in ChMap 2.1 currentTime() is set as a default lastBsTs; set to 0 here; TODO review
            resetLastBootstrapTimestamp(remoteIdentifier);

            modificationIterators.set(remoteIdentifier, newModificationIterator);
            modIterSet.set(remoteIdentifier);
            return newModificationIterator;
        }
    }

    public void onPut(Bytes key) {
        onChange(key, false);
    }

    public void onRemove(Bytes key) {
        onChange(key, true);
    }

    private void onChange(Bytes key, boolean deleted) {
        Value value = threadLocalValue.get();
        try (WriteContext<Bytes, Value> wc = map.acquireUsingLocked(key, value)) {
            value.setDeleted(deleted);
            long currentTs = timeProvider.currentTimeMillis();
            long entryTs = value.getTimestamp();
            if (entryTs > currentTs)
                currentTs = entryTs + 1;
            value.setTimestamp(currentTs);
            value.setIdentifier(identifier);
            raiseChange(value);
            for (long next = modIterSet.nextSetBit(0L); next > 0L;
                 next = modIterSet.nextSetBit(next + 1L)) {
                ChronicleMapBackedModificationIterator modIter =
                        modificationIterators.get((int) next);
                modIter.modNotify();
                if (modificationIteratorsRequiringSettingBootstrapTimestamp.clearIfSet(next)) {
                    if (!setNextBootstrapTimestamp((int) next, currentTs))
                        throw new AssertionError();
                }
            }
        }

    }

    class ChronicleMapBackedModificationIterator
            implements ModificationIterator, ReplicationEntry, BiConsumer<Bytes, Value> {

        private final int identifier;

        ChronicleMapBackedModificationIterator(int identifier) {
            this.identifier = identifier;
        }

        Consumer<ReplicationEntry> consumer;
        long forEachEntryCount;

        @Override
        public void forEach(@NotNull Consumer<ReplicationEntry> consumer)
                throws InterruptedException {
            this.consumer = consumer;
            forEachEntryCount = 0;
            try {
                map.forEach(this);
                if (forEachEntryCount == 0) {
                    modificationIteratorsRequiringSettingBootstrapTimestamp.set(identifier);
                    resetNextBootstrapTimestamp(identifier);
                }
            } finally {
                this.consumer = null;
            }
        }

        @Override
        public void accept(Bytes key, Value value) {
            if (isChanged(value, identifier)) {
                this.key = key;
                this.value = value;
                try {
                    consumer.accept(this);
                    clearChange(value, identifier);
                    forEachEntryCount++;
                } finally {
                    this.key = null;
                    this.value = null;
                }
            }
        }

        @Override
        public void dirtyEntries(long fromTimeStamp) throws InterruptedException {
            map.forEach((key, value) -> {
                if (value.getTimestamp() >= fromTimeStamp)
                    setChange(value, identifier);
            });
        }


        ModificationNotifier modificationNotifier;

        @Override
        public void setModificationNotifier(@NotNull ModificationNotifier modificationNotifier) {
            this.modificationNotifier = modificationNotifier;
        }

        public void modNotify() {
            if (modificationNotifier != null)
                modificationNotifier.onChange();
        }

        // Below methods and fields that implement ModIter as ReplicationEntry
        Bytes key;
        Value value;

        @Override
        public Bytes key() {
            return key;
        }

        @Override
        public Bytes value() {
            return getValue.getValue(store, key);
        }

        @Override
        public long timestamp() {
            return value.getTimestamp();
        }

        @Override
        public byte identifier() {
            return value.getIdentifier();
        }

        @Override
        public boolean isDeleted() {
            return value.getDeleted();
        }


        /**
         * @return the timestamp  that the remote client should bootstrap from when there has been a
         * disconnection, this time maybe later than the message time as event are not send in
         * chronological order from the bit set.
         */
        @Override
        public long bootStrapTimeStamp() {
            return bootstrapTimestamp(identifier);
        }
    }

    @Override
    public void close() throws IOException {
        map.close();
    }
}
