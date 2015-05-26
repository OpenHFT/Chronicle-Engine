package net.openhft.chronicle.engine2.api;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.Wire;

import java.util.function.Function;

/**
 * Created by peter on 24/05/15.
 */
public class FactoryContext<I extends Assetted> {
    public final Asset parent;
    private String queryString;
    private Class type, type2;
    private String name;
    private I item;
    private String basePath;
    private Function<Bytes, Wire> wireType = TextWire::new;

    private FactoryContext(Asset parent) {
        this.parent = parent;
    }

    public static FactoryContext<Assetted> factoryContext(Asset parent) {
        return new FactoryContext<>(parent);
    }

    public FactoryContext<I> name(String name) {
        this.name = name;
        return this;
    }

    public FactoryContext<I> queryString(String queryString) {
        this.queryString = queryString;
        return this;
    }

    public FactoryContext<I> type(Class type) {
        this.type = type;
        return this;
    }

    public Class type() {
        return type;
    }

    public FactoryContext type2(Class type2) {
        this.type2 = type2;
        return this;
    }

    public Class type2() {
        return type2;
    }

    public Asset parent() {
        return parent;
    }

    public String queryString() {
        return queryString;
    }

    public String name() {
        return name;
    }

    public I item() {
        return item == null ? parent == null ? null : (I) parent.item() : item;
    }

    public <Item extends Assetted> FactoryContext<Item> item(Item resource) {
        this.item = (I) resource;
        return (FactoryContext<Item>) this;
    }

    public FactoryContext<I> basePath(String basePath) {
        this.basePath = basePath;
        return this;
    }

    public String basePath() {
        return basePath;
    }

    public FactoryContext<I> wireType(Function<Bytes, Wire> writeType) {
        this.wireType = writeType;
        return this;
    }

    public Function<Bytes, Wire> wireType() {
        return wireType;
    }
}
