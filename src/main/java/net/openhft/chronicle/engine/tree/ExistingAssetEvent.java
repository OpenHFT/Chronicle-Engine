/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.Optional;

/**
 * Created by peter on 22/05/15.
 */
public class ExistingAssetEvent implements TopologicalEvent {
    private String assetName;
    private String name;

    private ExistingAssetEvent(String assetName, String name) {
        this.assetName = assetName;
        this.name = name;
    }

    @NotNull
    public static ExistingAssetEvent of(String assetName, String name) {
        return new ExistingAssetEvent(assetName, name);
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
                .filter(o -> o instanceof ExistingAssetEvent)
                .map(o -> (ExistingAssetEvent) o)
                .filter(e -> Objects.equals(assetName, e.assetName))
                .filter(e -> Objects.equals(name, e.name))
                .isPresent();
    }

    @NotNull
    @Override
    public String toString() {
        return "ExistingAssetEvent{" +
                "assetName='" + assetName + '\'' +
                ", name='" + name + '\'' +
                '}';
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        wire.read(TopologicalFields.assetName).text(s -> assetName = s);
        wire.read(TopologicalFields.name).text(s -> name = s);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(TopologicalFields.assetName).text(assetName);
        wire.write(TopologicalFields.name).object(name);
    }
}
