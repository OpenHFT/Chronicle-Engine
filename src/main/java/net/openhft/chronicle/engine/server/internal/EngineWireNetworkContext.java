/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.tree.HostIdentifier;
import net.openhft.chronicle.network.VanillaNetworkContext;
import org.jetbrains.annotations.NotNull;


/**
 * @author Rob Austin.
 */
public class EngineWireNetworkContext<T extends EngineWireNetworkContext> extends VanillaNetworkContext<T> {

    private Asset rootAsset;

    @NotNull
    public Asset rootAsset() {
        return rootAsset;
    }


    @Override
    public String toString() {

        byte localId = -1;
        if (rootAsset == null) {
            return "";
        }

        final Asset root = rootAsset;
        final HostIdentifier hostIdentifier = root.getView(HostIdentifier.class);

        if (hostIdentifier != null)
            localId = hostIdentifier.hostId();

        return "EngineWireNetworkContext{" +
                "localId=" + localId +
                "rootAsset=" + root +
                '}';
    }

    public EngineWireNetworkContext<T> rootAsset(Asset asset) {
        this.rootAsset = asset.root();
        return this;
    }
}

