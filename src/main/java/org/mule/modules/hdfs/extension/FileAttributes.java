/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.extension;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.mule.runtime.core.message.BaseAttributes;

/**
 * @author MuleSoft, Inc.
 */
public class FileAttributes extends BaseAttributes {

    private boolean pathExists;

    private ContentSummary contentSummary;

    private FileStatus fileStatus;

    private FileChecksum fileChecksum;

    public FileAttributes(boolean pathExists, ContentSummary contentSummary, FileStatus fileStatus, FileChecksum fileChecksum) {
        this.pathExists = pathExists;
        this.contentSummary = contentSummary;
        this.fileStatus = fileStatus;
        this.fileChecksum = fileChecksum;
    }
}
