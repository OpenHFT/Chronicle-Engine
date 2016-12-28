package net.openhft.chronicle.engine.api.column;

import net.openhft.chronicle.wire.AbstractMarshallable;
import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
public class VaadinChartSeries extends AbstractMarshallable {

    @NotNull
    public VaadinChartSeries width(Integer width) {
        this.width = width;
        return this;
    }

    private Integer width = 5;

    public String yAxisLabel() {
        return yAxisLabel;
    }

    @NotNull
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

    @NotNull
    public VaadinChartSeries field(String field) {
        this.field = field;
        return this;
    }

    @NotNull
    public VaadinChartSeries type(Type type) {
        this.type = type;
        return this;
    }
}
