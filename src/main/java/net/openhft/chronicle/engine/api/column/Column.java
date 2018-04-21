package net.openhft.chronicle.engine.api.column;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.wire.AbstractMarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
public class Column extends AbstractMarshallable {
    public final String name;
    public final boolean readOnly;
    public final boolean primaryKey;
    public final boolean sortable;
    public Object value;
    public transient Class<?> type;
    public String unaliased;
    public String typeName;

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

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        unaliased = type.getName();
        typeName = ClassAliasPool.CLASS_ALIASES.nameFor(type);
        super.writeMarshallable(wire);
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
        super.readMarshallable(wire);

        if (!unaliased.equals(typeName)) {
            try {
                type = Class.forName(unaliased);
                ClassAliasPool.CLASS_ALIASES.addAlias(type, typeName);
            } catch (ClassNotFoundException e) {
                Jvm.warn().on(getClass(), "unkown type type=" + unaliased);
                type = String.class;
            }
        }
    }
}
