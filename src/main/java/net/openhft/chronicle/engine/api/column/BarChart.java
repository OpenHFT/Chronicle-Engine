package net.openhft.chronicle.engine.api.column;

/**
 * @author Rob Austin.
 */
public interface BarChart {

    /**
     * the title of the chart
     */
    String title();

    /**
     * @return the name of the field in the column view that will be used to get the value of each
     * chartColumn
     */
    String columnValueField();

    /**
     * @return the name of the field in the column name that will be used to get the value of each
     * chartColumn
     */
    String columnNameField();

    ColumnViewInternal columnView();
}
