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

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.ReferenceCounted;
import org.jetbrains.annotations.Nullable;

/**
 * Created by daniel on 21/05/15.
 */
class FileRecord<T> {
    final long timestamp;
    private final T contents;
    boolean valid = true;

    FileRecord(long timestamp, T contents) {
        this.timestamp = timestamp;
        this.contents = contents;
    }

    @Nullable
    public T contents() {
        if (contents instanceof ReferenceCounted)
            try {
                ((ReferenceCounted) contents).reserve();
            } catch (IllegalStateException e) {
                return null;
            }
        return contents;
    }
}
