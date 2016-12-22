/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.filesystem;

import org.mule.modules.hdfs.filesystem.dto.FileSystemStatus;
import org.mule.modules.hdfs.filesystem.read.DataChunksConsumer;
import org.mule.modules.hdfs.filesystem.read.DataChunksReader;

import java.net.URI;

/**
 * @author MuleSoft, Inc.
 */
public interface MuleFileSystem {

    DataChunksConsumer openConsumer(URI path, long startPosition, int bufferSize);

    DataChunksReader openReader(URI path, long startPosition, int bufferSize);

    FileSystemStatus fileSystemStatus();
}
