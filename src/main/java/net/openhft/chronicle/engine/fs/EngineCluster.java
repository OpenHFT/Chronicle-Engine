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

package net.openhft.chronicle.engine.fs;

import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.cfg.EngineClusterContext;
import net.openhft.chronicle.network.cluster.Cluster;

/**
 * Created by peter.lawrey on 17/06/2015.
 */
public class EngineCluster extends Cluster<EngineHostDetails, EngineClusterContext> {

    public EngineCluster(String clusterName) {
        super(clusterName);
    }

    void assetRoot(Asset assetRoot) {
        EngineClusterContext context = clusterContext();
        if (context != null)
            context.assetRoot(assetRoot);
    }

    @Override
    protected EngineHostDetails newHostDetails() {
        return new EngineHostDetails();
    }
}
