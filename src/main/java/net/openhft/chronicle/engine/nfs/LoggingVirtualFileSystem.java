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

import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.nfs.v4.xdr.nfsace4;
import org.dcache.nfs.vfs.*;

import javax.security.auth.Subject;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * Created by peter on 23/08/15.
 */
public class LoggingVirtualFileSystem extends ForwardingFileSystem {
    private final VirtualFileSystem delegate;
    private final Consumer<String> logger;
    String lastGettattrMsg = "";

    public LoggingVirtualFileSystem(VirtualFileSystem delegate, Consumer<String> logger) {
        this.delegate = delegate;
        this.logger = logger;
    }

    @Override
    public int access(Inode inode, int mode) throws IOException {
        int access = super.access(inode, mode);
        logger.accept("access " + NFSUtils.toString(inode) + " " + Integer.toOctalString(mode) + " = " + Integer.toOctalString(access));
        return access;
    }

    @Override
    public Inode create(Inode parent, Stat.Type type, String path, Subject subject, int mode) throws IOException {
        Inode inode = super.create(parent, type, path, subject, mode);
        logger.accept("create " + NFSUtils.toString(parent) + " " + type + " " + path + " " + subject.toString().replaceAll("Principal: ", " ").replaceAll("\\s+", " ") + " " + Integer.toOctalString(mode) + " = " + NFSUtils.toString(inode));
        return inode;
    }

    @Override
    public FsStat getFsStat() throws IOException {
        FsStat fsStat = super.getFsStat();
        logger.accept("getFsStat = " + fsStat);
        return fsStat;
    }

    @Override
    public Inode getRootInode() throws IOException {
        Inode rootInode = super.getRootInode();
        logger.accept("getRootInode " + NFSUtils.toString(rootInode));
        return rootInode;
    }

    @Override
    public Inode lookup(Inode parent, String path) throws IOException {
        Inode lookup = super.lookup(parent, path);
        logger.accept("lookup " + NFSUtils.toString(parent) + " " + path + " = " + NFSUtils.toString(lookup));
        return lookup;
    }

    @Override
    public Inode link(Inode parent, Inode link, String path, Subject subject) throws IOException {
        Inode link1 = super.link(parent, link, path, subject);
        logger.accept("link " + NFSUtils.toString(parent) + " " + NFSUtils.toString(link) + " " + path + " " + subject + " = " + NFSUtils.toString(link1));
        return link1;
    }

    @Override
    public List<DirectoryEntry> list(Inode inode) throws IOException {
        List<DirectoryEntry> list = super.list(inode);
        logger.accept("list " + NFSUtils.toString(inode) + " = " + list);
        return list;
    }

    @Override
    public Inode mkdir(Inode parent, String path, Subject subject, int mode) throws IOException {
        Inode mkdir = super.mkdir(parent, path, subject, mode);
        logger.accept("mkdir " + NFSUtils.toString(parent) + " " + path + " " + subject + " " + Integer.toOctalString(mode) + " = " + NFSUtils.toString(mkdir));
        return mkdir;
    }

    @Override
    public boolean move(Inode src, String oldName, Inode dest, String newName) throws IOException {
        boolean move = super.move(src, oldName, dest, newName);
        logger.accept("move " + NFSUtils.toString(src) + " " + oldName + " " + NFSUtils.toString(dest) + " " + newName + " = " + move);
        return move;
    }

    @Override
    public Inode parentOf(Inode inode) throws IOException {
        Inode inode1 = super.parentOf(inode);
        logger.accept("parentOf " + NFSUtils.toString(inode) + " = " + inode1);
        return inode1;
    }

    @Override
    public int read(Inode inode, byte[] data, long offset, int count) throws IOException {
        int read = super.read(inode, data, offset, count);
        logger.accept("read " + NFSUtils.toString(inode) + " byte[" + data.length + "] " + offset + " " + count + " = " + read);
        return read;
    }

    @Override
    public String readlink(Inode inode) throws IOException {
        String readlink = super.readlink(inode);
        logger.accept("readlink " + NFSUtils.toString(inode) + " = " + readlink);
        return readlink;
    }

    @Override
    public void remove(Inode parent, String path) throws IOException {
        logger.accept("remove " + NFSUtils.toString(parent) + " " + path);
        super.remove(parent, path);
    }

    @Override
    public Inode symlink(Inode parent, String path, String link, Subject subject, int mode) throws IOException {
        Inode symlink = super.symlink(parent, path, link, subject, mode);
        logger.accept("symlink " + NFSUtils.toString(parent) + " " + path + " " + link + " " + subject + " " + Integer.toOctalString(mode) + " = " + NFSUtils.toString(symlink));
        return symlink;
    }

    @Override
    public WriteResult write(Inode inode, byte[] data, long offset, int count, StabilityLevel stabilityLevel) throws IOException {
        WriteResult write = super.write(inode, data, offset, count, stabilityLevel);
        logger.accept("write " + NFSUtils.toString(inode) + " byte[" + data.length + "] " + offset + " " + count + " " + stabilityLevel);
        return write;
    }

    @Override
    public void commit(Inode inode, long offset, int count) throws IOException {
        logger.accept("commit " + NFSUtils.toString(inode) + " " + offset + " " + count);
        super.commit(inode, offset, count);
    }

    @Override
    public Stat getattr(Inode inode) throws IOException {
        Stat getattr = super.getattr(inode);
        String getattrMsg = "getattr " + NFSUtils.toString(inode) + " = " + getattr;
        if (!getattrMsg.equals(lastGettattrMsg)) {
            logger.accept(getattrMsg);
            lastGettattrMsg = getattrMsg;
        }
        return getattr;
    }

    @Override
    public void setattr(Inode inode, Stat stat) throws IOException {
        logger.accept("setattr " + NFSUtils.toString(inode) + " " + stat);
        super.setattr(inode, stat);
    }

    @Override
    public nfsace4[] getAcl(Inode inode) throws IOException {
        nfsace4[] acl = super.getAcl(inode);
        logger.accept("getAcl " + NFSUtils.toString(inode) + " = " + Arrays.toString(acl));
        return acl;
    }

    @Override
    public void setAcl(Inode inode, nfsace4[] acl) throws IOException {
        logger.accept("setAcl " + NFSUtils.toString(inode) + " " + Arrays.toString(acl));
        super.setAcl(inode, acl);
    }

    @Override
    public boolean hasIOLayout(Inode inode) throws IOException {
        boolean b = super.hasIOLayout(inode);
        logger.accept("hasIOLayout " + NFSUtils.toString(inode) + " = " + b);
        return b;
    }

    @Override
    public AclCheckable getAclCheckable() {
        AclCheckable aclCheckable = super.getAclCheckable();
        logger.accept("getAclCheckable = " + aclCheckable);
        return aclCheckable;
    }

    @Override
    public NfsIdMapping getIdMapper() {
        NfsIdMapping idMapper = super.getIdMapper();
        logger.accept("getIdMapper= " + idMapper);
        return idMapper;
    }

    @Override
    protected VirtualFileSystem delegate() {
        return delegate;
    }
}
