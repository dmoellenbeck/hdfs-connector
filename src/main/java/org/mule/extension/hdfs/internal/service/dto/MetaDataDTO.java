package org.mule.extension.hdfs.internal.service.dto;

public class MetaDataDTO {

    private boolean pathExists;
    private ContentSummaryDTO contentSummary;
    private FileStatusDTO fileStatus;
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

    public FileStatusDTO getFileStatus() {
        return fileStatus;
    }

    public void setFileStatus(FileStatusDTO fileStatus) {
        this.fileStatus = fileStatus;
    }

    public CheckSummaryDTO getCheckSummary() {
        return checkSummary;
    }

    public void setCheckSummary(CheckSummaryDTO checkSummary) {
        this.checkSummary = checkSummary;
    }
}
