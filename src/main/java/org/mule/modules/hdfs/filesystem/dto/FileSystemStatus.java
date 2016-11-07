/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.filesystem.dto;

/**
 * @author MuleSoft, Inc.
 */
public class FileSystemStatus {

    private long capacity;
    private long used;
    private long remaining;

    public FileSystemStatus(long capacity, long used, long remaining) {
        this.capacity = capacity;
        this.used = used;
        this.remaining = remaining;
    }

    public long getCapacity() {
        return capacity;
    }

    public long getUsed() {
        return used;
    }

    public long getRemaining() {
        return remaining;
    }
}
