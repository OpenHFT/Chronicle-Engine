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

import net.openhft.chronicle.engine.api.map.ValueReader;
import net.openhft.chronicle.engine.api.tree.Asset;

/**
 * Created by peter on 17/09/15.
 */
public class VanillaSubAssetFactory implements SubAssertFactory {
    @Override
    public <E> Asset createSubAsset(VanillaAsset asset, String name, Class<E> valueType) {
        @SuppressWarnings("unchecked")
        ValueReader<Object, E> vr = asset.getView(ValueReader.class);
        return new VanillaSubAsset<E>(asset, name, valueType, vr);
    }
}
