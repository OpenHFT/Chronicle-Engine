package net.openhft.chronicle.engine.api.column;

import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.tree.QueueView;
import net.openhft.chronicle.wire.AbstractMarshallable;

/**
 * @author Rob Austin.
 */
public class VanillaBarChart extends AbstractMarshallable implements BarChart {
    private String columnNameField;
    private String columnValueField;
    private String title;
    private ColumnViewInternal columnView;

    public VanillaBarChart(RequestContext requestContext, Asset asset) {

    }

    public String columnNameField() {
        return columnNameField;
    }

    public VanillaBarChart dataSource(MapView mapView) {
        this.columnView = mapView.asset().acquireView(MapColumnView.class);
        return this;
    }

    public VanillaBarChart dataSource(QueueView mapView) {
        this.columnView = mapView.asset().acquireView(QueueColumnView.class);
        return this;
    }

    @Override
    public ColumnViewInternal columnView() {
        return columnView;
    }

    public VanillaBarChart columnNameField(String columnNameField) {
        this.columnNameField = columnNameField;
        return this;
    }

    public String columnValueField() {
        return columnValueField;
    }

    public VanillaBarChart columnValueField(String columnValueField) {
        this.columnValueField = columnValueField;
        return this;
    }

    public String title() {
        return title;
    }

    public VanillaBarChart title(String title) {
        this.title = title;
        return this;
    }


}
