package net.openhft.chronicle.engine.map;

import java.io.Closeable;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Created by Rob Austin
 */
public interface ClosableMapSupplier<K, V> extends Closeable, Supplier<Map<K,V>> {

}
