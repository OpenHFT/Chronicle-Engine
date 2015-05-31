package net.openhft.chronicle.engine2.api;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.engine2.api.collection.ValuesCollection;
import net.openhft.chronicle.engine2.api.map.MapView;
import net.openhft.chronicle.engine2.api.set.EntrySetView;
import net.openhft.chronicle.engine2.api.set.KeySetView;
import net.openhft.chronicle.wire.*;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by peter on 24/05/15.
 */
public class RequestContext {
    private String pathName;
    private String name;
    private Class viewType, type, type2;
    private String basePath;
    private Function<Bytes, Wire> wireType = TextWire::new;
    private Boolean putReturnsNull = null,
            removeReturnsNull = null,
            bootstrap = null;
    private double averageValueSize;
    private long entries;

    private static final Map<String, Class> classAliases = new ConcurrentHashMap<>();

    private static void addAlias(Class type, String aliases) {
        for (String alias : aliases.split(", ?")) {
            classAliases.put(alias, type);
            classAliases.put(Character.toLowerCase(alias.charAt(0)) + alias.substring(1), type);
            classAliases.put(alias.toLowerCase(), type);
        }
    }

    static {
        addAlias(MapView.class, "Map");
        addAlias(EntrySetView.class, "EntrySet");
        addAlias(KeySetView.class, "KeySet");
        addAlias(ValuesCollection.class, "Values");
        addAlias(Set.class, "Set");
        addAlias(Publisher.class, "Publisher, Pub");
        addAlias(TopicPublisher.class, "TopicPublisher, TopicPub");
        addAlias(Reference.class, "Reference, Ref");
        addAlias(String.class, "String");
        addAlias(Bytes.class, "Byte, int8");
        addAlias(Character.class, "Character, Char");
        addAlias(Integer.class, "Integer, int32");
        addAlias(Long.class, "Long, Int, int64");
        addAlias(Float.class, "Float, Float32");
        addAlias(Double.class, "Double, Float64");
    }

    private RequestContext() {
    }

    public RequestContext(String pathName, String name) {
        this.pathName = pathName;
        this.name = name;
    }

    public static RequestContext requestContext() {
        return new RequestContext();
    }

    public static RequestContext requestContext(String uri) {
        int queryPos = uri.indexOf('?');
        String fullName = queryPos >= 0 ? uri.substring(0, queryPos) : uri;
        String query = queryPos >= 0 ? uri.substring(queryPos + 1) : "";
        int dirPos = fullName.lastIndexOf('/');
        String pathName = dirPos >= 0 ? fullName.substring(0, dirPos) : "";
        String name = dirPos >= 0 ? fullName.substring(dirPos + 1) : fullName;
        return new RequestContext(pathName, name).queryString(query);
    }

    public RequestContext queryString(String queryString) {
        if (queryString.isEmpty())
            return this;
        WireParser parser = new VanillaWireParser();
        parser.register(() -> "view", v -> v.text((Consumer<String>) this::view));
        parser.register(() -> "bootstrap", v -> v.bool(b -> this.bootstrap = b));
        parser.register(() -> "putReturnsNull", v -> v.bool(b -> this.putReturnsNull = b));
        parser.register(() -> "removeReturnsNull", v -> v.bool(b -> this.removeReturnsNull = b));
        parser.register(() -> "basePath", v -> v.text((Consumer<String>) x -> this.basePath = x));
        parser.register(() -> "viewType", v -> v.typeLiteral(this::lookupType, x -> this.viewType = x));
        parser.register(() -> "keyType", v -> v.typeLiteral(this::lookupType, x -> this.type = x));
        parser.register(() -> "valueType", v -> v.typeLiteral(this::lookupType, x -> this.type2 = x));
        parser.register(() -> "elementType", v -> v.typeLiteral(this::lookupType, x -> this.type = x));
        parser.register(WireParser.DEFAULT, ValueIn.DISCARD);
        Bytes bytes = Bytes.from(queryString);
        QueryWire wire = new QueryWire(bytes);
        while (bytes.remaining() > 0)
            parser.parse(wire);
        return this;
    }

    RequestContext view(String viewName) {
        try {
            Class clazz = lookupType(viewName);
            viewType(clazz);
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("Unknown view name:" + viewName);
        }
        return this;
    }

    Class lookupType(CharSequence typeName) throws IllegalArgumentException {
        String typeNameStr = typeName.toString();
        Class clazz = classAliases.get(typeNameStr);
        if (clazz != null)
            return clazz;
        try {
            clazz = Class.forName(typeNameStr);
            classAliases.put(typeNameStr, clazz);
            return clazz;
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Unknown class name:" + typeNameStr);
        }
    }

    public RequestContext type(Class type) {
        this.type = type;
        return this;
    }

    public Class type() {
        return type;
    }

    public Class elementType() {
        return type;
    }

    public Class keyType() {
        return type;
    }

    public Class valueType() {
        return type2;
    }

    public RequestContext type2(Class type2) {
        this.type2 = type2;
        return this;
    }

    public Class type2() {
        return type2;
    }

    public String fullName() {
        return pathName.isEmpty() ? name : (pathName + "/" + name);
    }

    public RequestContext basePath(String basePath) {
        this.basePath = basePath;
        return this;
    }

    public String basePath() {
        return basePath;
    }

    public RequestContext wireType(Function<Bytes, Wire> writeType) {
        this.wireType = writeType;
        return this;
    }

    public Function<Bytes, Wire> wireType() {
        return wireType;
    }

    public String namePath() {
        return pathName;
    }

    public String name() {
        return name;
    }

    public double getAverageValueSize() {
        return averageValueSize;
    }

    public RequestContext averageValueSize(double averageValueSize) {
        this.averageValueSize = averageValueSize;
        return this;
    }

    public long getEntries() {
        return entries;
    }

    public RequestContext entries(long entries) {
        this.entries = entries;
        return this;
    }

    public RequestContext name(String name) {
        this.name = name;
        return this;
    }

    public RequestContext viewType(Class assetType) {
        this.viewType = assetType;
        return this;
    }


    public Class viewType() {
        return viewType;
    }

    public RequestContext fullName(String fullName) {
        int dirPos = fullName.lastIndexOf('/');
        this.pathName = dirPos >= 0 ? fullName.substring(0, dirPos) : "";
        this.name = dirPos >= 0 ? fullName.substring(dirPos + 1) : fullName;
        return this;
    }

    public Boolean putReturnsNull() {
        return putReturnsNull;
    }

    public Boolean removeReturnsNull() {
        return removeReturnsNull;
    }

    public Boolean bootstrap() {
        return bootstrap;
    }

    public RequestContext bootstrap(boolean bootstrap) {
        this.bootstrap = bootstrap;
        return this;
    }

    @Override
    public String toString() {
        return "RequestContext{" +
                "pathName='" + pathName + '\'' +
                ", name='" + name + '\'' +
                ", viewType=" + viewType +
                ", type=" + type +
                ", type2=" + type2 +
                ", basePath='" + basePath + '\'' +
                ", wireType=" + wireType +
                ", putReturnsNull=" + putReturnsNull +
                ", removeReturnsNull=" + removeReturnsNull +
                ", bootstrap=" + bootstrap +
                '}';
    }
}
