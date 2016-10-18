package net.openhft.chronicle.engine.api.column;

import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Rob Austin.
 */
public class Row {

    private List<String> columnNames;
    private Map<String, Object> data = new LinkedHashMap<>();

    /**
     * @param columnNames all the column names that make up this row
     */
    public Row(@NotNull List<String> columnNames) {
        this.columnNames = columnNames;
    }

    /**
     * @return the value in the cell denoted by the {@code columnName}  in this row
     */
    public Object get(String columnName) {
        return data.get(columnName);
    }

    /**
     * @return the value in the cell denoted by the {@code columnIndex}  in this row
     */
    public Object get(int columnIndex) {
        return data.get(columnNames.get(columnIndex));
    }

    /**
     * sets the value in a cell
     *
     * @param columnName the name of the cell that is being set in this row
     * @param newValue   the new value that is being store
     */
    public void set(String columnName, Object newValue) {
        assert columnNames.contains(columnName);
        data.put(columnName, newValue);
    }
}
