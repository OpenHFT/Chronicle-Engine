package net.openhft.chronicle.engine.api.column;

import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.tree.QueueView;
import net.openhft.chronicle.wire.AbstractMarshallable;

/**
 * @author Rob Austin.
 */
public class VanillaVaadinChart extends AbstractMarshallable implements VaadinChart {
    private String columnNameField;
    private VaadinChartType[] columnValueFields;
    private BarChartProperties barChartProperties;
    private ColumnViewInternal columnView;

    public VanillaVaadinChart(RequestContext requestContext, Asset asset) {

    }

    public String columnNameField() {
        return columnNameField;
    }

    public VanillaVaadinChart dataSource(MapView mapView) {
        this.columnView = mapView.asset().acquireView(MapColumnView.class);
        return this;
    }

    public VanillaVaadinChart dataSource(QueueView mapView) {
        this.columnView = mapView.asset().acquireView(QueueColumnView.class);
        return this;
    }

    @Override
    public ColumnViewInternal columnView() {
        return columnView;
    }

    public VanillaVaadinChart columnNameField(String columnNameField) {
        this.columnNameField = columnNameField;
        return this;
    }

    public VaadinChartType[] columnValueField() {
        return columnValueFields;
    }

    public VanillaVaadinChart columnValueFields(VaadinChartType... columnValueFields) {
        this.columnValueFields = columnValueFields;
        return this;
    }

    public BarChartProperties barChartProperties() {
        return barChartProperties;
    }

    public VanillaVaadinChart barChartProperties(BarChartProperties barChartProperties) {
        this.barChartProperties = barChartProperties;
        return this;
    }


}
