package net.openhft.chronicle.engine.api.column;

import net.openhft.chronicle.wire.AbstractMarshallable;

/**
 * @author Rob Austin.
 */
public class VaadinChartSeries extends AbstractMarshallable {

    public VaadinChartSeries width(Integer width) {
        this.width = width;
        return this;
    }

    private Integer width = 5;

    public String yAxisLabel() {
        return yAxisLabel;
    }

    public VaadinChartSeries yAxisLabel(String yAxisLabel) {
        this.yAxisLabel = yAxisLabel;
        return this;
    }

    String yAxisLabel = "";

    public Type type() {
        return type;
    }

    public Integer width() {
        return width;
    }

    public enum Type {SPLINE, COLUMN}

    public String field;

    public Type type = Type.COLUMN;



    public VaadinChartSeries(String field) {
        this.field = field;
    }

    public String field() {
        return field;
    }

    public VaadinChartSeries field(String field) {
        this.field = field;
        return this;
    }

    public VaadinChartSeries type(Type type) {
        this.type = type;
        return this;
    }
}
