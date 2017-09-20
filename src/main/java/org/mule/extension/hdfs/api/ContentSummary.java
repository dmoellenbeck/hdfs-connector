package org.mule.extension.hdfs.api;


public class ContentSummary {


    private long length;
    private long fileCount;
    private long directoryCount;
    private long snapshotLength;
    private long snapshotFileCount;
    private long snapshotDirectoryCount;
    private long snapshotSpaceConsumed;

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public long getFileCount() {
        return fileCount;
    }

    public void setFileCount(long fileCount) {
        this.fileCount = fileCount;
    }

    public long getDirectoryCount() {
        return directoryCount;
    }

    public void setDirectoryCount(long directoryCount) {
        this.directoryCount = directoryCount;
    }

    public long getSnapshotLength() {
        return snapshotLength;
    }

    public void setSnapshotLength(long snapshotLength) {
        this.snapshotLength = snapshotLength;
    }

    public long getSnapshotFileCount() {
        return snapshotFileCount;
    }

    public void setSnapshotFileCount(long snapshotFileCount) {
        this.snapshotFileCount = snapshotFileCount;
    }

    public long getSnapshotDirectoryCount() {
        return snapshotDirectoryCount;
    }

    public void setSnapshotDirectoryCount(long snapshotDirectoryCount) {
        this.snapshotDirectoryCount = snapshotDirectoryCount;
    }

    public long getSnapshotSpaceConsumed() {
        return snapshotSpaceConsumed;
    }

    public void setSnapshotSpaceConsumed(long snapshotSpaceConsumed) {
        this.snapshotSpaceConsumed = snapshotSpaceConsumed;
    }

}
