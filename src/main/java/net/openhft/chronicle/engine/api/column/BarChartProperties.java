package net.openhft.chronicle.engine.api.column;


import net.openhft.chronicle.wire.AbstractMarshallable;

import java.util.function.Function;

/**
 * @author Rob Austin.
 */
public class BarChartProperties extends AbstractMarshallable {
    public String menuLabel = "chart";

    // the Legend shown at the top of the chart
    public String title = "";

    // set set, will replace the yAxis title
    public String yAxisTitle = "";

    public ColumnViewInternal.MarshableFilter filter;

    // if not ZERO, will cause the ChartUI to plot data from the end of the ColumnView.
    public long countFromEnd;

    // if not null the ChartUI will use this function to render the field-name
    public Function<Object, String> xAxisLableRender;
}
