package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.column.BarChart;
import net.openhft.chronicle.engine.api.column.ColumnViewInternal;
import net.openhft.chronicle.engine.api.column.RemoteBarChart;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static net.openhft.chronicle.network.connection.CoreFields.reply;

/**
 * @author Rob Austin.
 */
public class BarChartHandler extends AbstractHandler {
    private final CspManager cspManager;
    AtomicLong nextToken = new AtomicLong();

    BarChartHandler(CspManager cspManager) {
        this.cspManager = cspManager;
    }

    private final StringBuilder eventName = new StringBuilder();
    private BarChart barChart;

    @Nullable
    private WireIn inWire = null;

    private long tid;


    private final BiConsumer<WireIn, Long> dataConsumer = new BiConsumer<WireIn, Long>() {

        @SuppressWarnings("ConstantConditions")
        @Override
        public void accept(WireIn wireIn, Long inputTid) {

            eventName.setLength(0);
            inWire.readEventName(eventName);

            try {

                outWire.writeDocument(true, wire -> outWire.writeEventName(CoreFields.tid).int64(tid));

                writeData(inWire.bytes(), out -> {

                    if (RemoteBarChart.EventId.title.contentEquals(eventName)) {
                        outWire.writeEventName(reply).text(barChart.title());
                        return;
                    }

                    if (RemoteBarChart.EventId.columnValueField.contentEquals(eventName)) {
                        outWire.writeEventName(reply).text(barChart.columnValueField());
                        return;
                    }

                    if (RemoteBarChart.EventId.columnNameField.contentEquals(eventName)) {
                        outWire.writeEventName(reply).text(barChart.columnNameField());
                        return;
                    }
                    if (RemoteBarChart.EventId.columnView.contentEquals(eventName)) {
                        ColumnViewInternal columnView = barChart.columnView();
                        outWire.writeEventName(reply).text(columnView.asset().fullName());
                        return;
                    }


                    throw new IllegalStateException("unsupported event=" + eventName);
                });

            } catch (Exception e) {
                Jvm.warn().on(getClass(), e);
            }


        }
    };


    public void process(WireIn in, WireOut out, BarChart barChart, long tid) {

        setOutWire(out);

        try {
            this.inWire = in;
            this.outWire = out;
            this.barChart = barChart;
            this.tid = tid;
            dataConsumer.accept(in, tid);
        } catch (Exception e) {
            Jvm.warn().on(getClass(), "", e);
        }
    }
}
