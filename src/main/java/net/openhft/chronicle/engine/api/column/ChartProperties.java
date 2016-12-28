package net.openhft.chronicle.engine.api.column;


import net.openhft.chronicle.wire.AbstractMarshallable;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

/**
 * @author Rob Austin.
 */
public class ChartProperties extends AbstractMarshallable {
    @NotNull
    public String menuLabel = "chart";

    // the Legend shown at the top of the chart
    @NotNull
    public String title = "";

    // set set, will replace the yAxis title
    @NotNull
    public String yAxisTitle = "";

    public ColumnViewInternal.MarshableFilter filter;

    // if not ZERO, will cause the ChartUI to plot data from the end of the ColumnView.
    public long countFromEnd;

    // if not null the ChartUI will use this function to render the field-name
    public Function<Object, String> xAxisLabelRender;
}
