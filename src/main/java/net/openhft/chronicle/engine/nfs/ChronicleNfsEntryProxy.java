package net.openhft.chronicle.engine.nfs;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.Nullable;

/**
 * holds a reference to the map and the key of interest, the reason that we don hold a reference to
 * the entry is that chronicle map stores its entries off heap so holding just the entry is unlikely
 * to work in all cases
 */
class ChronicleNfsEntryProxy {
    private final MapView mapView;
    private final String key;
    private CharSequence text;
    private long lastTimeMS = 0;
    private boolean readOnly;

    public ChronicleNfsEntryProxy(MapView mapView, String key) {
        this.mapView = mapView;
        this.key = key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ChronicleNfsEntryProxy)) return false;

        ChronicleNfsEntryProxy that = (ChronicleNfsEntryProxy) o;

        return !(mapView != null ? !mapView.equals(that.mapView) : that.mapView != null)
                && !(key != null ? !key.equals(that.key) : that.key != null);

    }

    @Override
    public int hashCode() {
        int result = mapView != null ? mapView.hashCode() : 0;
        result = 31 * result + (key != null ? key.hashCode() : 0);
        return result;
    }

    public MapView mapView() {
        return mapView;
    }

    public String key() {
        return key;
    }

    public int valueSize() {
        final CharSequence o = value();
        if (o == null)
            return 0;
        return o.length();
    }

    @Nullable
    public CharSequence value() {
        long now = System.currentTimeMillis();
        if (lastTimeMS + 1 < now) {
            Object value = mapView.get(key);
            if (value instanceof WriteMarshallable) {
                Bytes bytes = Bytes.elasticByteBuffer();
                TextWire wire = new TextWire(bytes);
                wire.writeObject(value);
                text = bytes;
            } else if (value instanceof CharSequence) {
                text = (CharSequence) value;
            } else if (value == null) {
                text = null;
            } else {
                text = value.toString();
            }
            lastTimeMS = now;
        }
        return text;
    }

    public boolean isReadOnly() {
        return mapView.valueType() != String.class;
    }
}
