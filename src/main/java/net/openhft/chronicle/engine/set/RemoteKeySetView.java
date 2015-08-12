package net.openhft.chronicle.engine.set;

import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.query.Query;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.query.RemoteQuery;
import org.jetbrains.annotations.NotNull;

import static java.util.EnumSet.of;
import static net.openhft.chronicle.engine.api.tree.RequestContext.Operation.BOOTSTRAP;
import static net.openhft.chronicle.engine.api.tree.RequestContext.Operation.END_SUBSCRIPTION_AFTER_BOOTSTRAP;

/**
 * @author Rob Austin.
 */
public class RemoteKeySetView<K, V> extends VanillaKeySetView<K, V> {

    private MapView<K, ?> mapView;

    public RemoteKeySetView(@NotNull RequestContext context,
                            @NotNull Asset asset,
                            @NotNull MapView mapView) {
        super(context, asset, mapView);
        this.mapView = mapView;
    }


    @Override
    @NotNull
    public Query<K> query() {
        return new RemoteQuery<>((subscriber, filter1, contextOperations) -> mapView.registerKeySubscriber(
                subscriber,
                filter1,
                of(BOOTSTRAP, END_SUBSCRIPTION_AFTER_BOOTSTRAP)));
    }
}
