package org.mule.extension.hdfs.api;

import org.mule.extension.hdfs.internal.service.dto.CheckSummaryDTO;
import org.mule.extension.hdfs.internal.service.dto.ContentSummaryDTO;

import java.io.Serializable;

public class MetaData implements Serializable {

    private boolean pathExists;
    private ContentSummaryDTO contentSummary;
    private FileStatus fileStatus;
    private CheckSummaryDTO checkSummary;

    public boolean isPathExists() {
        return pathExists;
    }

    public void setPathExists(boolean pathExists) {
        this.pathExists = pathExists;
    }

    public ContentSummaryDTO getContentSummary() {
        return contentSummary;
    }

    public void setContentSummary(ContentSummaryDTO contentSummary) {
        this.contentSummary = contentSummary;
    }

    public FileStatus getFileStatus() {
        return fileStatus;
    }

    public void setFileStatus(FileStatus fileStatus) {
        this.fileStatus = fileStatus;
    }

    public CheckSummaryDTO getCheckSummary() {
        return checkSummary;
    }

    public void setCheckSummary(CheckSummaryDTO checkSummary) {
        this.checkSummary = checkSummary;
    }
}
