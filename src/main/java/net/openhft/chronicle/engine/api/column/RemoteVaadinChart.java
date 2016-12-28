package net.openhft.chronicle.engine.api.column;

import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.network.connection.AbstractStatelessClient;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.ParameterizeWireKey;
import net.openhft.chronicle.wire.WireKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static net.openhft.chronicle.engine.api.column.RemoteVaadinChart.EventId.*;

/**
 * @author Rob Austin.
 */
public class RemoteVaadinChart extends AbstractStatelessClient implements VaadinChart {
    @NotNull
    private final RequestContext context;
    @NotNull
    private final Asset asset;

    public RemoteVaadinChart(@NotNull RequestContext context, @NotNull Asset asset) {
        super(asset.findView(TcpChannelHub.class), (long) 0, toURL(context));
        this.context = context;
        this.asset = asset;
    }

    @NotNull
    private static String toURL(@NotNull RequestContext context) {
        return context.viewType(VaadinChart.class).toUri();
    }

    /**
     * the chartProperties of the chart
     */
    @Nullable
    @Override
    public ChartProperties chartProperties() {
        return (ChartProperties) proxyReturnTypedObject(EventId.chartProperties, null, ChartProperties.class);
    }

    /**
     * @return the name of the field in the column view that will be used to get the value of each
     * chartColumn
     */
    @Nullable
    @Override
    public VaadinChartSeries[] series() {
        return (VaadinChartSeries[]) proxyReturnTypedObject(series, null, VaadinChartSeries[].class);
    }

    /**
     * @return the name of the field in the column name that will be used to get the value of each
     * chartColumn
     */
    @Nullable
    @Override
    public String columnNameField() {
        return (String) proxyReturnTypedObject(columnNameField, null, String.class);
    }

    /**
     * @return the column view used to build the chart
     */
    @NotNull
    public ColumnView columnView() {
        @Nullable String url = (String) proxyReturnTypedObject(columnView, null, String.class);
        return asset.acquireView(RequestContext.requestContext(url).viewType
                (ColumnView.class));
    }

    public enum EventId implements ParameterizeWireKey {
        chartProperties,
        series,     // used only by the queue view
        columnNameField,
        columnView;
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
