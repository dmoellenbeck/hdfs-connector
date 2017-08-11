package org.mule.extension.hdfs.api.operation.param;

import java.io.InputStream;

import org.mule.runtime.extension.api.annotation.param.Content;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;

public class WriteOpParams {

    @Parameter
    private String path;
    
    @Parameter
    @Optional(defaultValue = "700")
    private String permission;

    @Parameter
    @Optional(defaultValue = "true")
    private boolean overwrite;

    @Parameter
    @Optional(defaultValue = "4096")
    private int bufferSize;
    
    @Parameter
    @Optional(defaultValue = "1")
    private int replication;

    @Parameter
    @Optional(defaultValue = "1048576")
    private long blockSize;

    @Parameter
    @Optional
    private String ownerUserName;

    @Parameter
    @Optional
    private String ownerGroupName;

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
