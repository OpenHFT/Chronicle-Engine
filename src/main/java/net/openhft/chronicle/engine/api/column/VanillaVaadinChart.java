package net.openhft.chronicle.engine.api.column;

import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.tree.QueueView;
import net.openhft.chronicle.wire.AbstractMarshallable;
import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
public class VanillaVaadinChart extends AbstractMarshallable implements VaadinChart {
    private String columnNameField;
    private VaadinChartSeries[] series;
    private ChartProperties chartProperties;
    private ColumnViewInternal columnView;

    public VanillaVaadinChart(RequestContext requestContext, Asset asset) {

    }

    @Override
    public String columnNameField() {
        return columnNameField;
    }

    @NotNull
    public VanillaVaadinChart dataSource(@NotNull MapView mapView) {
        this.columnView = mapView.asset().acquireView(MapColumnView.class);
        return this;
    }

    @NotNull
    public VanillaVaadinChart dataSource(@NotNull QueueView mapView) {
        this.columnView = mapView.asset().acquireView(QueueColumnView.class);
        return this;
    }

    @Override
    public ColumnViewInternal columnView() {
        return columnView;
    }

    @NotNull
    public VanillaVaadinChart columnNameField(String columnNameField) {
        this.columnNameField = columnNameField;
        return this;
    }

    @Override
    public VaadinChartSeries[] series() {
        return series;
    }

    @NotNull
    public VanillaVaadinChart series(VaadinChartSeries... series) {
        this.series = series;
        return this;
    }

    @Override
    public ChartProperties chartProperties() {
        return chartProperties;
    }

    @NotNull
    public VanillaVaadinChart chartProperties(ChartProperties chartProperties) {
        this.chartProperties = chartProperties;
        return this;
    }

}
