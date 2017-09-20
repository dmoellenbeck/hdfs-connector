package org.mule.extension.hdfs.internal.service.dto;

import org.apache.hadoop.io.MD5Hash;

public class CheckSummaryDTO  {
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

    public MD5Hash getMd5() {
        return md5;
    }

    public void setMd5(MD5Hash md5) {
        this.md5 = md5;
    }

    private int bytesPerCRC;
    private long crcPerBlock;
    private MD5Hash md5;
}
