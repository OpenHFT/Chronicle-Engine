package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.column.ColumnView;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static net.openhft.chronicle.engine.api.column.RemoteColumnView.EventId.*;
import static net.openhft.chronicle.network.connection.CoreFields.reply;

/**
 * @author Rob Austin.
 */
class ColumnViewHandler extends AbstractHandler {

    private final CspManager cspManager;

    public ColumnViewHandler(CspManager cspManager) {
        this.cspManager = cspManager;
    }

    private static final Logger LOG = LoggerFactory.getLogger(MapWireHandler.class);
    private final StringBuilder eventName = new StringBuilder();
    private final StringBuilder cpsBuff = new StringBuilder();
    private ColumnView columnView;

    @NotNull
    private final Map<Long, String> cidToCsp = new HashMap<>();
    @NotNull
    private final Map<String, Long> cspToCid = new HashMap<>();
    private final AtomicLong cid = new AtomicLong();

    private RequestContext requestContext;

    @Nullable
    private WireIn inWire = null;
    @Nullable
    private Map<String, Object> oldRow = new HashMap<String, Object>();
    private Map<String, Object> newRow = new HashMap<String, Object>();

    private long tid;
    private List<ColumnView.MarshableFilter> filters = new ArrayList<>();
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
                        valueIn.marshallable(wire -> {
                            wire.read(rowCount.params()[0]).object(filters, List.class);
                            columnView.rowCount(filters);
                        });
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
                        valueIn.marshallable(wire -> {
                            final Object[] keys = (Object[]) wire.read(containsRowWithKey.params()[0]).object();
                            final boolean result = columnView.containsRowWithKey(keys);
                            outWire.writeEventName(reply).bool(result);
                        });
                        return;
                    }

                    if (iterator.contentEquals(eventName)) {
                        valueIn.marshallable(wire -> {
                            wire.read(iterator.params()[0]).object(sortedFilter, ColumnView.SortedFilter.class);
                            final long cid = cspManager.createProxy(eventName.toString());
                            cspManager.storeObject(cid, columnView.iterator(sortedFilter));
                            outWire.writeEventName(reply).int32(cid);
                        });
                        return;
                    }

                    throw new IllegalStateException("unsupported event=" + eventName);
                });

            } catch (Exception e) {
                Jvm.warn().on(getClass(), e);
            }
        }
    };


    public void process(WireIn in, WireOut out, ColumnView view, long tid) {

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

