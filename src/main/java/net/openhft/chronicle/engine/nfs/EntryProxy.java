package net.openhft.chronicle.engine.nfs;

import net.openhft.chronicle.engine.api.map.MapView;

/**
 * holds a reference to the map and the key of interest the reason that we don hold a reference to
 * the entry is that chronicle map stores its entries off heap
 */
class EntryProxy {
    MapView mapView;
    String key;

    public EntryProxy(MapView mapView, String key) {
        this.mapView = mapView;
        this.key = key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EntryProxy)) return false;

        EntryProxy that = (EntryProxy) o;

        if (mapView != null ? !mapView.equals(that.mapView) : that.mapView != null)
            return false;
        return !(key != null ? !key.equals(that.key) : that.key != null);

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
        final Object o = value();
        if (o == null)
            return 0;
        return o.toString().length();
    }

    public Object value() {
        return mapView.get(key);
    }

    public void remove() {
        mapView.remove(key);
    }
}
