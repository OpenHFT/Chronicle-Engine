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

package net.openhft.chronicle.engine.collection;

/**
 * Created by Rob Austin
 */

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StreamCorruptedException;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static net.openhft.chronicle.wire.Wires.acquireStringBuilder;

/**
 * @author Rob Austin.
 */
public class CollectionWireHandlerProcessor<U, C extends Collection<U>> implements
        CollectionWireHandler<U, C> {

    public static final int SIZE_OF_SIZE = 4;
    private static final Logger LOG = LoggerFactory.getLogger(CollectionWireHandlerProcessor.class);
    private Function<ValueIn, U> fromWire;
    private BiConsumer<ValueOut, U> toWire;

    @Nullable
    private Wire inWire = null;
    @Nullable
    private Wire outWire = null;
    private C underlyingCollection;
    private long tid;
    private Supplier<C> factory;

    private final Consumer<WireIn> dataConsumer = new Consumer<WireIn>() {
        @Override
        public void accept(WireIn wireIn) {
            final Bytes<?> outBytes = outWire.bytes();

            try {

                final StringBuilder eventName = acquireStringBuilder();
                final ValueIn valueIn = inWire.readEventName(eventName);

                outWire.writeDocument(true, wire -> outWire.writeEventName(CoreFields.tid).int64
                        (CollectionWireHandlerProcessor.this.tid));

                outWire.writeDocument(false, out -> {

                    // note :  remove on the key-set returns a boolean and on the map returns the
                    // old value
                    if (SetEventId.remove.contentEquals(eventName)) {
                        outWire.write(CoreFields.reply).bool(underlyingCollection.remove(fromWire.apply(valueIn)));
                        return;
                    }

                    // note :  remove on the key-set returns a boolean and on the map returns the
                    // old value
                    if (SetEventId.iterator.contentEquals(eventName)) {
                        final ValueOut valueOut = outWire.writeEventName(CoreFields.reply);
                        valueOut.sequence(v -> underlyingCollection.forEach(e -> toWire.accept(v, e)));
                        return;
                    }

                    if (SetEventId.numberOfSegments.contentEquals(eventName)) {
                        outWire.write(CoreFields.reply).int32(1);
                        return;
                    }

                    if (SetEventId.isEmpty.contentEquals(eventName)) {
                        outWire.write(CoreFields.reply).bool(underlyingCollection.isEmpty());
                        return;
                    }

                    if (SetEventId.size.contentEquals(eventName)) {
                        outWire.write(CoreFields.reply).int32(underlyingCollection.size());
                        return;
                    }

                    if (SetEventId.clear.contentEquals(eventName)) {
                        underlyingCollection.clear();
                        return;
                    }

                    if (SetEventId.contains.contentEquals(eventName)) {
                        outWire.write(CoreFields.reply).bool(
                                underlyingCollection.contains(fromWire.apply(valueIn)));
                        return;
                    }

                    if (SetEventId.add.contentEquals(eventName)) {
                        outWire.write(CoreFields.reply).bool(
                                underlyingCollection.add(fromWire.apply(valueIn)));
                        return;
                    }

                    if (SetEventId.remove.contentEquals(eventName)) {
                        outWire.write(CoreFields.reply).bool(
                                underlyingCollection.remove(fromWire.apply(valueIn)));
                        return;
                    }

                    if (SetEventId.containsAll.contentEquals(eventName)) {
                        outWire.write(CoreFields.reply).bool(
                                underlyingCollection.remove(collectionFromWire()));
                        return;
                    }

                    if (SetEventId.addAll.contentEquals(eventName)) {
                        outWire.write(CoreFields.reply).bool(
                                underlyingCollection.addAll(collectionFromWire()));
                        return;
                    }

                    if (SetEventId.removeAll.contentEquals(eventName)) {
                        outWire.write(CoreFields.reply).bool(
                                underlyingCollection.removeAll(collectionFromWire()));
                        return;
                    }

                    if (SetEventId.retainAll.contentEquals(eventName)) {
                        outWire.write(CoreFields.reply).bool(
                                underlyingCollection.retainAll(collectionFromWire()));
                        return;
                    }

                    throw new IllegalStateException("unsupported event=" + eventName);
                });
            } catch (Exception e) {
                LOG.error("", e);
            } finally {

                if (YamlLogging.showServerWrites) {
                    long len = outBytes.writePosition() - SIZE_OF_SIZE;
                    if (len == 0) {
                        LOG.info(
                                "\n\nserver writes:\n\n<EMPTY>");

                    } else {

                     LOG.info(
                                "server writes:\n\n" +
                                        //      Bytes.toDebugString(outBytes, SIZE_OF_SIZE, len));
                                        Wires.fromSizePrefixedBlobs(outBytes, SIZE_OF_SIZE, len));
                    }
                }
            }
        }
    };

    private C collectionFromWire() {
        C c = factory.get();
        final ValueIn valueIn = outWire.getValueIn();
        while (valueIn.hasNextSequenceItem()) {
            c.add(fromWire.apply(valueIn));
        }
        return c;
    }

    @Override
    public void process(@NotNull Wire in,
                        @NotNull Wire out,
                        @NotNull C collection,
                        @NotNull CharSequence csp,
                        @NotNull BiConsumer<ValueOut, U> toWire,
                        @NotNull Function<ValueIn, U> fromWire,
                        @NotNull Supplier<C> factory,
                        long tid) throws StreamCorruptedException {

        this.fromWire = fromWire;
        this.toWire = toWire;
        this.underlyingCollection = collection;
        this.factory = factory;

        try {
            this.inWire = in;
            this.outWire = out;
            this.tid = tid;
            dataConsumer.accept(in);
        } catch (Exception e) {
            LOG.error("", e);
        }
    }
}

