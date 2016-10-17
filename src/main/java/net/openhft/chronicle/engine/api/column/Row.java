package net.openhft.chronicle.engine.api.column;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Rob Austin.
 */
public class Row {
    private List<String> columnNames;
    public Map<String, Object> data = new LinkedHashMap<>();

    public Row(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public Object cell(String columnName) {
        return data.get(columnName);
    }

    public Object cell(int columnIndex) {
        return data.get(columnNames.get(columnIndex));
    }

    public void add(String columnName, Object value) {
        assert columnNames.contains(columnName);
        data.put(columnName, value);
    }
}
