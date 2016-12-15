package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.column.Row;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.BiConsumer;

import static net.openhft.chronicle.engine.server.internal.ColumnViewIteratorHandler.EventId.close;
import static net.openhft.chronicle.engine.server.internal.ColumnViewIteratorHandler.EventId.next;
import static net.openhft.chronicle.network.connection.CoreFields.reply;

/**
 * @author Rob Austin.
 */
public class ColumnViewIteratorHandler extends AbstractHandler {

    private final CspManager cspManager;
    private Iterator<Row> iterator;
    private long cid;

    ColumnViewIteratorHandler(CspManager cspManager) {
        this.cspManager = cspManager;
    }

    private final StringBuilder eventName = new StringBuilder();

    public void process(WireIn in, WireOut out, long tid, Iterator<Row> iterator, long cid) {


        setOutWire(out);

        try {
            this.inWire = in;
            this.outWire = out;
            this.iterator = iterator;
            this.tid = tid;
            this.cid = cid;
            dataConsumer.accept(in, tid);
        } catch (Exception e) {
            Jvm.warn().on(getClass(), "", e);
        }
    }


    private long tid;


    @Nullable
    private WireIn inWire = null;
    @Nullable

    private final BiConsumer<WireIn, Long> dataConsumer = new BiConsumer<WireIn, Long>() {

        @SuppressWarnings("ConstantConditions")
        @Override
        public void accept(WireIn wireIn, Long inputTid) {

            eventName.setLength(0);
            final ValueIn valueIn = inWire.readEventName(eventName);

            try {

                outWire.writeDocument(true, wire -> outWire.writeEventName(CoreFields.tid).int64(tid));

                writeData(inWire, out -> {

                    if (next.contentEquals(eventName)) {

                        int nextChunkSize = valueIn.int32();

                        final ArrayList<Row> chunk = new ArrayList<>();

                        for (int i = 0; i < nextChunkSize; i++) {
                            if (!iterator.hasNext())
                                break;
                            chunk.add(iterator.next());
                        }

                        outWire.writeEventName(reply).object(chunk);
                        return;
                    }

                    if (close.contentEquals(eventName)) {
                        cspManager.removeCid(cid);
                        return;
                    }

                    throw new IllegalStateException("unsupported event=" + eventName);
                });

            } catch (Exception e) {
                Jvm.warn().on(getClass(), e);
            }
        }
    };


    public enum EventId implements ParameterizeWireKey {
        next,
        close;

        private final WireKey[] params;

        <P extends WireKey> EventId(P... params) {
            this.params = params;
        }

        @NotNull
        public <P extends WireKey> P[] params() {
            return (P[]) this.params;
        }
    }


}
