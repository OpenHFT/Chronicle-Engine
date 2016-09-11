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

package net.openhft.chronicle.engine.api.map;

import net.openhft.chronicle.engine.api.tree.Asset;

/**
 * This interface represents as Asset which are keyed components of a parent.
 * <p></p>
 * e.g. if the Parent in a Map, this represents on of the entries in that map.
 */
public interface SubAsset<T> extends Asset {
}
