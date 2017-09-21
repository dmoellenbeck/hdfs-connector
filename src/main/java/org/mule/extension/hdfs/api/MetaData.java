/**
 * (c) 2003-2017 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.extension.hdfs.api;

import java.io.Serializable;

public class MetaData implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Indicates if the path exists
     */
    private boolean pathExists;
    /**
     * A resume of the path info
     */
    private ContentSummary contentSummary;
    /**
     * info about the status of the file
     */
    private FileStatus fileStatus;

    /**
     * MD5 digest of the file (if it is a file and exists)
     */
    private CheckSummary checkSummary;

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
