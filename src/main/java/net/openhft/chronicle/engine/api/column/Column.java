package net.openhft.chronicle.engine.api.column;

import net.openhft.chronicle.wire.AbstractMarshallable;

/**
 * @author Rob Austin.
 */
public class Column extends AbstractMarshallable{
    public final String name;
    public boolean readOnly;
    public boolean primaryKey;
    public Object value;
    public Class<?> type;
    public boolean canShort;

    public Column(String name, boolean readOnly, boolean primaryKey, Object value, Class<?> type, boolean canSort) {
        this.name = name;
        this.readOnly = readOnly;
        this.primaryKey = primaryKey;
        this.value = value;
        this.type = type;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public  Class<?>  type() {
             return type;
    }
}
