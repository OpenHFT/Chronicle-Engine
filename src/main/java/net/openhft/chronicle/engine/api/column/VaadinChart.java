package net.openhft.chronicle.engine.api.column;

/**
 * @author Rob Austin.
 */
public interface VaadinChart {

    /**
     * the barChartProperties of the chart
     */
    BarChartProperties barChartProperties();

    /**
     * @return the name of the field in the column view that will be used to get the value of each
     * chartColumn
     */
    VaadinChartType[] columnValueField();

    /**
     * @return the name of the field in the column name that will be used to get the value of each
     * chartColumn
     */
    String columnNameField();

    ColumnViewInternal columnView();
}
