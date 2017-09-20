package org.mule.extension.hdfs.api;

public class MetaData {

    boolean pathExists;
    ContentSummary contentSummary;
    FileStatus fileStatus;
    CheckSummary checkSummary;


    public boolean isPathExists() {
        return pathExists;
    }

    public void setPathExists(boolean pathExists) {
        this.pathExists = pathExists;
    }

    public ContentSummary getContentSummary() {
        return contentSummary;
    }

    public void setContentSummary(ContentSummary contentSummary) {
        this.contentSummary = contentSummary;
    }

    public FileStatus getFileStatus() {
        return fileStatus;
    }

    public void setFileStatus(FileStatus fileStatus) {
        this.fileStatus = fileStatus;
    }

    public CheckSummary getCheckSummary() {
        return checkSummary;
    }

    public void setCheckSummary(CheckSummary checkSummary) {
        this.checkSummary = checkSummary;
    }
}
