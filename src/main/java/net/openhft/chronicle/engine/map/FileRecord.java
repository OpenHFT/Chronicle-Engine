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

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.ReferenceCounted;
import org.jetbrains.annotations.Nullable;

/**
 * Created by daniel on 21/05/15.
 */
class FileRecord<T> {
    final long timestamp;
    boolean valid = true;
    private final T contents;

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
