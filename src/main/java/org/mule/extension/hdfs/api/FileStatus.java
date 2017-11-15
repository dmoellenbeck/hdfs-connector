/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.api;

public class FileStatus {

    private String path;

    private long length;

    private boolean isDirectory;

    private short blockReplication;

    private long blockSize;

    private long modificationTime;

    private long accessTime;

    private String permission;

    private String owner;

    private String group;

    private String symbolicLink;


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

    public boolean isDirectory() {
        return isDirectory;
    }

    public void setDirectory(boolean directory) {
        this.isDirectory = directory;
    }

    public short getBlockReplication() {
        return blockReplication;
    }

    public void setBlockReplication(short blockReplication) {
        this.blockReplication = blockReplication;
    }

    public long getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(long blockSize) {
        this.blockSize = blockSize;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    public void setModificationTime(long modificationTime) {
        this.modificationTime = modificationTime;
    }

    public long getAccessTime() {
        return accessTime;
    }

    public void setAccessTime(long accessTime) {
        this.accessTime = accessTime;
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

    public String getSymbolicLink() {
        return symbolicLink;
    }

    public void setSymbolicLink(String symbolicLink) {
        this.symbolicLink = symbolicLink;
    }

    public boolean isSymbolicLink() {
        return symbolicLink != null;
    }
}
