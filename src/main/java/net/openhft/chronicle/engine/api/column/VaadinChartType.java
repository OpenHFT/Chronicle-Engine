package net.openhft.chronicle.engine.api.column;

import net.openhft.chronicle.wire.AbstractMarshallable;

/**
 * @author Rob Austin.
 */
public class VaadinChartType extends AbstractMarshallable {

    public String yAxisLabel() {
        return yAxisLabel;
    }

    public VaadinChartType yAxisLabel(String yAxisLabel) {
        this.yAxisLabel = yAxisLabel;
        return this;
    }

    String yAxisLabel = "";

    public Type type() {
        return type;
    }

    public enum Type {SPLINE, COLUMN}

    public String field;

    public Type type = Type.COLUMN;


    public VaadinChartType(String field) {
        this.field = field;
    }

    public String field() {
        return field;
    }

    public VaadinChartType field(String field) {
        this.field = field;
        return this;
    }

    public VaadinChartType type(Type type) {
        this.type = type;
        return this;
    }
}
