/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.filesystem.dto;

/**
 * @author MuleSoft, Inc.
 */
public class DataChunk {

    private long startByte;

    private int bytesRead;

    private byte[] data;

    public DataChunk(long startByte, int bytesRead, byte[] data) {
        this.startByte = startByte;
        this.bytesRead = bytesRead;
        this.data = data;
    }

    public long getStartByte() {
        return startByte;
    }

    public int getBytesRead() {
        return bytesRead;
    }

    public byte[] getData() {
        return data;
    }
}
