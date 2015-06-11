package net.openhft.chronicle.engine.tree;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.Optional;

/**
 * Created by peter on 22/05/15.
 */
public class AddedAssetEvent implements TopologicalEvent {
    private final String assetName;
    private final String name;

    private AddedAssetEvent(String assetName, String name) {
        this.assetName = assetName;
        this.name = name;
    }

    @NotNull
    public static AddedAssetEvent of(String assetName, String name) {
        return new AddedAssetEvent(assetName, name);
    }

    @Override
    public boolean added() {
        return true;
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
        return Objects.hash("added", assetName, name);
    }

    @Override
    public boolean equals(Object obj) {
        return Optional.ofNullable(obj)
                .filter(o -> o instanceof AddedAssetEvent)
                .map(o -> (AddedAssetEvent) o)
                .filter(e -> Objects.equals(assetName, e.assetName))
                .filter(e -> Objects.equals(name, e.name))
                .isPresent();
    }

    @Override
    public String toString() {
        return "AddedAssetEvent{" +
                "assetName='" + assetName + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
