package net.openhft.chronicle.engine.api.query;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.tree.QueueView;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class Client implements Closeable {

    private final AtomicReference<Throwable> t = new AtomicReference<>();
    @NotNull
    private final AssetTree client;
    @NotNull
    private final IndexQueueView<Subscriber<IndexedValue<Marshallable>>, Marshallable> indexQueueView;
    private final TypeToString typeToString;

    public Client(@NotNull String uri, @NotNull final String[] hosts, TypeToString typeToString) {
        client = new VanillaAssetTree().forRemoteAccess(hosts, WireType.BINARY);
        indexQueueView = client.acquireAsset(uri).acquireView(IndexQueueView.class);
        this.typeToString = typeToString;
    }

    public <T extends Marshallable> void subscribes(final @NotNull Class<T> valueClass,
                                                    final @NotNull String filter,
                                                    final long fromIndex,
                                                    final @NotNull Consumer<IndexedValue<T>> consumer) {
        @NotNull final VanillaIndexQuery indexQuery = new VanillaIndexQuery<>();
        indexQuery.select(valueClass, filter);
        indexQuery.fromIndex(fromIndex);
        indexQuery.eventName(typeToString.typeToString(valueClass));
        @NotNull Subscriber<IndexedValue<Marshallable>> accept = t1 -> consumer.accept(
                (IndexedValue) t1.deepCopy());
        indexQueueView.registerSubscriber(accept, indexQuery);
    }

    public <T extends Marshallable> void remoteQueuePut(@NotNull final String uri, final @NotNull T value) {
        QueueView q = client.acquireQueue(uri, String.class, Marshallable.class, "clusterTwo");
        q.publishAndIndex(typeToString.typeToString(value.getClass()), value);
    }

    public Throwable throwable() {
        return t.get();
    }

    @Override
    public void close() {
        client.close();
    }
}
