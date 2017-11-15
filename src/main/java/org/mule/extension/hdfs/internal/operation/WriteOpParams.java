/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.internal.operation;

import java.io.InputStream;

import org.mule.runtime.extension.api.annotation.param.Content;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;

public class WriteOpParams {

    /**
     * the path of the file to write to.
     */
    @Parameter
    private String path;

    /**
     * the file system permission to use if a new file is created, either in octal or symbolic format (umask).
     */
    @Parameter
    @Optional(defaultValue = "700")
    private String permission;

    /**
     * if a pre-existing file should be overwritten with the new content.
     */
    @Parameter
    @Optional(defaultValue = "true")
    private boolean overwrite;

    /**
     * the buffer size to use when appending to the file.
     */
    @Parameter
    @Optional(defaultValue = "4096")
    private int bufferSize;

    /**
     * block replication for the file.
     */
    @Parameter
    @Optional(defaultValue = "1")
    private int replication;

    /**
     * the buffer size to use when appending to the file.
     */
    @Parameter
    @Optional(defaultValue = "1048576")
    private long blockSize;

    /**
     * the username owner of the file.
     */
    @Parameter
    @Optional
    private String ownerUserName;

    /**
     * the group owner of the file.
     */
    @Parameter
    @Optional
    private String ownerGroupName;

    /**
     * the payload to write to the file.
     */
    @Parameter
    @Content
    private InputStream payload;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getPermission() {
        return permission;
    }

    public void setPermission(String permission) {
        this.permission = permission;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public void setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public int getReplication() {
        return replication;
    }

    public void setReplication(int replication) {
        this.replication = replication;
    }

    public long getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(long blockSize) {
        this.blockSize = blockSize;
    }

    public String getOwnerUserName() {
        return ownerUserName;
    }

    public void setOwnerUserName(String ownerUserName) {
        this.ownerUserName = ownerUserName;
    }

    public String getOwnerGroupName() {
        return ownerGroupName;
    }

    public void setOwnerGroupName(String ownerGroupName) {
        this.ownerGroupName = ownerGroupName;
    }

    public InputStream getPayload() {
        return payload;
    }

    public void setPayload(InputStream payload) {
        this.payload = payload;
    }

}
