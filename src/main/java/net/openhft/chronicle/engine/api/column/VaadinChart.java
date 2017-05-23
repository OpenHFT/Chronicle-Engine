package net.openhft.chronicle.engine.api.column;

import org.jetbrains.annotations.Nullable;

/**
 * @author Rob Austin.
 */
public interface VaadinChart {

    /**
     * the chartProperties of the chart
     */
    @Nullable
    ChartProperties chartProperties();

    /**
     * @return the name of the field in the column view that will be used to get the value of each
     * chartColumn
     */
    @Nullable
    VaadinChartSeries[] series();

    /**
     * @return the name of the field in the column name that will be used to get the value of each
     * chartColumn
     */
    @Nullable
    String columnNameField();

    ColumnViewInternal columnView();
}
