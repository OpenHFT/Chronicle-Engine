package net.openhft.chronicle.engine.api.column;

import net.openhft.chronicle.wire.AbstractMarshallable;
import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
public class VaadinChartSeries extends AbstractMarshallable {

    public String field;
    public Type type = Type.COLUMN;
    String yAxisLabel = "";
    private Integer width = 5;

    public VaadinChartSeries(String field) {
        this.field = field;
    }

    @NotNull
    public VaadinChartSeries width(Integer width) {
        this.width = width;
        return this;
    }

    public String yAxisLabel() {
        return yAxisLabel;
    }

    @NotNull
    public VaadinChartSeries yAxisLabel(String yAxisLabel) {
        this.yAxisLabel = yAxisLabel;
        return this;
    }

    public Type type() {
        return type;
    }

    public Integer width() {
        return width;
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

    public enum Type {SPLINE, COLUMN}
}
