package net.openhft.chronicle.engine.api.column;

/**
 * @author Rob Austin.
 */
public class Column {
    public final String name;
    public boolean readOnly;
    public boolean allowReadOnlyChange;
    public boolean nullable;
    public boolean primaryKey;
    public Object value;
    public  Class<?> type;

    public Column(String propertyId, boolean readOnly, boolean allowReadOnlyChange, boolean nullable, boolean primaryKey, Object value, Class<?> type) {
        this.name = propertyId;
        this.readOnly = readOnly;
        this.allowReadOnlyChange = allowReadOnlyChange;
        this.nullable = nullable;
        this.primaryKey = primaryKey;
        this.value = value;
        this.type = type;
    }
}
