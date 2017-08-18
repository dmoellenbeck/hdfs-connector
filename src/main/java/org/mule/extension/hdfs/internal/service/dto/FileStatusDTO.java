/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.internal.service.dto;

public class FileStatusDTO {

    private String path;
    private long length;
    private boolean isdir;
    private short block_replication;
    private long blocksize;
    private long modification_time;
    private long access_time;
    private String permission;
    private String owner;
    private String group;
    private String symlink;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public boolean isIsdir() {
        return isdir;
    }

    public void setIsdir(boolean isdir) {
        this.isdir = isdir;
    }

    public short getBlock_replication() {
        return block_replication;
    }

    public void setBlock_replication(short block_replication) {
        this.block_replication = block_replication;
    }

    public long getBlocksize() {
        return blocksize;
    }

    public void setBlocksize(long blocksize) {
        this.blocksize = blocksize;
    }

    public long getModification_time() {
        return modification_time;
    }

    public void setModification_time(long modification_time) {
        this.modification_time = modification_time;
    }

    public long getAccess_time() {
        return access_time;
    }

    public void setAccess_time(long access_time) {
        this.access_time = access_time;
    }

    public String getPermission() {
        return permission;
    }

    public void setPermission(String permission) {
        this.permission = permission;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getSymlink() {
        return symlink;
    }

    public void setSymlink(String symlink) {
        this.symlink = symlink;
    }
   
    public boolean isSymlink() {
        return symlink != null;
      }
}
