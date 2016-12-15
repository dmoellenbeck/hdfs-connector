/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.extension.dto;

import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;

/**
 * @author MuleSoft, Inc.
 */
public class ReadParameters {

    /**
     * The path of the file to pool the content from.
     */
    @Parameter
    private String path;

    /**
     * Position from where to start reading.
     */
    @Parameter
    @Optional(defaultValue = "0")
    private Long startPosition;

    /**
     * Size of one chunk of data.
     */
    @Parameter
    @Optional(defaultValue = "4096")
    private Integer chunkSize;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Long getStartPosition() {
        return startPosition;
    }

    public void setStartPosition(Long startPosition) {
        this.startPosition = startPosition;
    }

    public Integer getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(Integer chunkSize) {
        this.chunkSize = chunkSize;
    }

}
