/*
 * Copyright 2014-2017 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.wire.Marshallable;
import org.jetbrains.annotations.NotNull;

public interface AssetRuleProvider extends Marshallable {
    void configMapCommon(@NotNull VanillaAsset asset);

    void configMapServer(@NotNull VanillaAsset asset);

    void configMapRemote(@NotNull VanillaAsset asset);

    void configQueueCommon(@NotNull VanillaAsset asset);

    void configQueueServer(@NotNull VanillaAsset asset);

    void configQueueRemote(@NotNull VanillaAsset asset);

    void configColumnViewRemote(@NotNull VanillaAsset asset);

    /**
     * return true if you are ok for an asset to be created here
     *
     * @param path the path the asset that is being create
     * @return {@code true} if it's ok to create an asset here
     */
    default boolean canCreateAsset(CharSequence path) {
        return true;
    }
}
