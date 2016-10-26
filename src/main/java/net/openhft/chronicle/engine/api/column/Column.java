package net.openhft.chronicle.engine.api.column;

import net.openhft.chronicle.wire.AbstractMarshallable;

/**
 * @author Rob Austin.
 */
public class Column extends AbstractMarshallable {
    public final String name;
    public final boolean readOnly;
    public final boolean primaryKey;
    public Object value;
    public final Class<?> type;
    public final boolean sortable;

    public Column(String name, boolean readOnly, boolean primaryKey, Object value, Class<?> type, boolean sortable) {
        this.name = name;
        this.readOnly = readOnly;
        this.primaryKey = primaryKey;
        this.value = value;
        this.type = type;
        this.sortable = sortable;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public Class<?> type() {
        return type;
    }
}
