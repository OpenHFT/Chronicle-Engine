package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.query.Query;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.query.Filter;
import net.openhft.chronicle.engine.query.RemoteQuery;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

import static java.util.EnumSet.of;
import static net.openhft.chronicle.engine.api.tree.RequestContext.Operation.BOOTSTRAP;
import static net.openhft.chronicle.engine.api.tree.RequestContext.Operation.END_SUBSCRIPTION_AFTER_BOOTSTRAP;

/**
 * @author Rob Austin.
 */
public class RemoteEntrySetView<K, MV, V> extends VanillaEntrySetView<K, MV, V> {
    public RemoteEntrySetView(RequestContext context, Asset asset, @NotNull MapView<K, V> mapView) throws AssetNotFoundException {
        super(context, asset, mapView);
    }


    @Override
    @NotNull
    public Query<Map.Entry<K, V>> query() {
        return new RemoteQuery<>((subscriber, filter, contextOperations) -> {
            mapView.registerSubscriber((Subscriber) subscriber, (Filter) filter, of
                    (BOOTSTRAP, END_SUBSCRIPTION_AFTER_BOOTSTRAP));

        });
    }
}
