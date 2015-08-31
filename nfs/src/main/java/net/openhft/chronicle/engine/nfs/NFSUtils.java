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

package net.openhft.chronicle.engine.nfs;

import org.dcache.nfs.vfs.FileHandle;
import org.dcache.nfs.vfs.Inode;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by peter on 24/08/15.
 */
public enum NFSUtils {
    ;

    public static String toString(Inode inode) {
        try {
            Field fhField = Inode.class.getDeclaredField("fh");
            fhField.setAccessible(true);
            FileHandle fh = (FileHandle) fhField.get(inode);
            byte[] fsOpaque = fh.getFsOpaque();
            String fsos;
            if (fsOpaque.length == 8)
                fsos = Long.toHexString(ByteBuffer.wrap(fsOpaque).getLong());
            else
                fsos = Arrays.toString(fsOpaque);
            return "v:" + Integer.toHexString(fh.getVersion())
                    + ",m:" + Integer.toHexString(fh.getMagic())
                    + ",g:" + Integer.toHexString(fh.getGeneration())
                    + ",i:" + Integer.toHexString(fh.getExportIdx())
                    + ",t:" + Integer.toHexString(fh.getType())
                    + ",o:" + fsos;
        } catch (Exception e) {
            return String.valueOf(inode);
        }
    }
}
