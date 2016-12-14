/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.filesystem;

import org.mule.modules.hdfs.filesystem.dto.DataChunk;
import org.mule.modules.hdfs.filesystem.dto.FileSystemStatus;

import java.net.URI;
import java.util.function.Consumer;

/**
 * @author MuleSoft, Inc.
 */
public interface MuleFileSystem {

    void consume(URI path, long startPosition, int bufferSize, Consumer<DataChunk> consumer);

    FileSystemStatus fileSystemStatus();
}
