/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.engine.api.map.ValueReader;
import net.openhft.chronicle.engine.api.tree.Asset;

/**
 * Created by peter on 17/09/15.
 */
public class VanillaSubAssetFactory implements SubAssetFactory {
    @Override
    public <E> Asset createSubAsset(VanillaAsset asset, String name, Class<E> valueType) {
        @SuppressWarnings("unchecked")
        ValueReader<Object, E> vr = asset.getView(ValueReader.class);
        return new VanillaSubAsset<E>(asset, name, valueType, vr);
    }
}
