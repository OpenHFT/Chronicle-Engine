package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.Optional;

/**
 * Created by peter on 22/05/15.
 */
public class RemovedAssetEvent implements TopologicalEvent {
    private String assetName;
    private String name;

    private RemovedAssetEvent(String assetName, String name) {
        this.assetName = assetName;
        this.name = name;
    }

    @NotNull
    public static RemovedAssetEvent of(String assetName, String name) {
        return new RemovedAssetEvent(assetName, name);
    }

    @Override
    public boolean added() {
        return false;
    }

    @Override
    public String assetName() {
        return assetName;
    }

    public String name() {
        return name;
    }

    @Override
    public int hashCode() {
        return Objects.hash("removal", assetName, name);
    }

    @Override
    public boolean equals(Object obj) {
        return Optional.ofNullable(obj)
                .filter(o -> o instanceof RemovedAssetEvent)
                .map(o -> (RemovedAssetEvent) o)
                .filter(e -> Objects.equals(assetName, e.assetName))
                .filter(e -> Objects.equals(name, e.name))
                .isPresent();
    }

    @Override
    public String toString() {
        return "RemovedAssetEvent{" +
                "assetName='" + assetName + '\'' +
                ", name='" + name + '\'' +
                '}';
    }


    @Override
    public void readMarshallable(WireIn wire) throws IllegalStateException {
        wire.read(TopologicalFields.assetName).text(s -> assetName = s);
        wire.read(TopologicalFields.name).text(s -> name = s);
    }

    @Override
    public void writeMarshallable(WireOut wire) {
        wire.write(TopologicalFields.assetName).text(assetName);
        wire.write(TopologicalFields.name).object(name);
    }
}
