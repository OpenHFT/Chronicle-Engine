package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.column.ColumnView;
import net.openhft.chronicle.engine.api.column.ColumnViewInternal;
import net.openhft.chronicle.engine.api.column.Row;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static net.openhft.chronicle.engine.api.column.RemoteColumnView.EventId.*;
import static net.openhft.chronicle.network.connection.CoreFields.reply;

/**
 * @author Rob Austin.
 */
class ColumnViewHandler extends AbstractHandler {

    private final CspManager cspManager;
    AtomicLong nextToken = new AtomicLong();

    ColumnViewHandler(CspManager cspManager) {
        this.cspManager = cspManager;
    }

    private final StringBuilder eventName = new StringBuilder();
    private ColumnViewInternal columnView;

    @Nullable
    private WireIn inWire = null;
    @Nullable
    private Map<String, Object> oldRow = new HashMap<String, Object>();
    private Map<String, Object> newRow = new HashMap<String, Object>();

    private long tid;
    private List<ColumnView.MarshableFilter> filtersList = new ArrayList<>();
    private List<ColumnView.MarshableFilter> keysList = new ArrayList<>();
    private ColumnView.SortedFilter sortedFilter = new ColumnView.SortedFilter();


    private final BiConsumer<WireIn, Long> dataConsumer = new BiConsumer<WireIn, Long>() {

        @SuppressWarnings("ConstantConditions")
        @Override
        public void accept(WireIn wireIn, Long inputTid) {

            eventName.setLength(0);
            final ValueIn valueIn = inWire.readEventName(eventName);

            try {

                outWire.writeDocument(true, wire -> outWire.writeEventName(CoreFields.tid).int64(tid));

                writeData(inWire.bytes(), out -> {

                    if (columns.contentEquals(eventName)) {
                        outWire.writeEventName(reply).object(columnView.columns());
                        return;
                    }

                    if (rowCount.contentEquals(eventName)) {
                        filtersList.clear();
                        List<ColumnView.MarshableFilter> filters = valueIn.object(filtersList, List.class);
                        int count = columnView.rowCount(filters == null ? Collections
                                .emptyList() : filters);
                        outWire.writeEventName(reply).int32(count);

                        return;
                    }

                    if (changedRow.contentEquals(eventName)) {
                        valueIn.marshallable(wire -> {
                            wire.read(rowCount.params()[0]).object(newRow, Map.class);
                            wire.read(rowCount.params()[1]).object(oldRow, Map.class);
                            final int result = columnView.changedRow(newRow, oldRow);
                            outWire.writeEventName(reply).int32(result);
                        });
                        return;
                    }

                    if (canDeleteRows.contentEquals(eventName)) {
                        outWire.writeEventName(reply).bool(columnView.canDeleteRows());
                        return;
                    }

                    if (containsRowWithKey.contentEquals(eventName)) {
                        keysList.clear();
                        final List keys = valueIn.object(keysList, List.class);
                        final boolean result = columnView.containsRowWithKey(keys);
                        outWire.writeEventName(reply).bool(result);

                        return;
                    }

                    if (iterator.contentEquals(eventName)) {
                        valueIn.marshallable(sortedFilter);
                        long token = nextToken.incrementAndGet();
                        final long cid = cspManager.createProxy("ColumnViewIterator", token);
                        final Iterator<? extends Row> iterator = columnView.iterator(sortedFilter);
                        cspManager.storeObject(cid, iterator);
                        //});
                        return;
                    }

                    throw new IllegalStateException("unsupported event=" + eventName);
                });

            } catch (   Exception e){
                Jvm.warn().on(getClass(), e);
            }

        }
    };


    public void process(WireIn in, WireOut out, ColumnViewInternal view, long tid) {

        setOutWire(out);

        try {
            this.inWire = in;
            this.outWire = out;
            this.columnView = view;
            this.tid = tid;
            dataConsumer.accept(in, tid);
        } catch (Exception e) {
            Jvm.warn().on(getClass(), "", e);
        }
    }
}

