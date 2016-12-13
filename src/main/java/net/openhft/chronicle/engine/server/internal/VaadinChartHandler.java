package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.column.ColumnViewInternal;
import net.openhft.chronicle.engine.api.column.RemoteVaadinChart;
import net.openhft.chronicle.engine.api.column.VaadinChart;
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
public class VaadinChartHandler extends AbstractHandler {
    private final CspManager cspManager;
    AtomicLong nextToken = new AtomicLong();

    VaadinChartHandler(CspManager cspManager) {
        this.cspManager = cspManager;
    }

    private final StringBuilder eventName = new StringBuilder();
    private VaadinChart vaadinChart;

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

                    if (RemoteVaadinChart.EventId.chartProperties.contentEquals(eventName)) {
                        outWire.writeEventName(reply).typedMarshallable(vaadinChart.chartProperties());
                        return;
                    }

                    if (RemoteVaadinChart.EventId.series.contentEquals(eventName)) {
                        outWire.writeEventName(reply).object(vaadinChart.series());
                        return;
                    }

                    if (RemoteVaadinChart.EventId.columnNameField.contentEquals(eventName)) {
                        outWire.writeEventName(reply).text(vaadinChart.columnNameField());
                        return;
                    }
                    if (RemoteVaadinChart.EventId.columnView.contentEquals(eventName)) {
                        ColumnViewInternal columnView = vaadinChart.columnView();
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


    public void process(WireIn in, WireOut out, VaadinChart vaadinChart, long tid) {

        setOutWire(out);

        try {
            this.inWire = in;
            this.outWire = out;
            this.vaadinChart = vaadinChart;
            this.tid = tid;
            dataConsumer.accept(in, tid);
        } catch (Exception e) {
            Jvm.warn().on(getClass(), "", e);
        }
    }
}
