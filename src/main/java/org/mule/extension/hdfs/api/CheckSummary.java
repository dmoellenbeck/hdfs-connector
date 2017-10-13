/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.api;

import java.io.Serializable;

public class CheckSummary implements Serializable {

    private int bytesPerCRC;
    private long crcPerBlock;
    private String md5;

    public int getBytesPerCRC() {
        return bytesPerCRC;
    }

    public void setBytesPerCRC(int bytesPerCRC) {
        this.bytesPerCRC = bytesPerCRC;
    }

    public long getCrcPerBlock() {
        return crcPerBlock;
    }

    public void setCrcPerBlock(long crcPerBlock) {
        this.crcPerBlock = crcPerBlock;
    }

    public String getMd5() {
        return md5;

    }

    public void setMd5(String md5) {
        this.md5 = md5;
    }

}
