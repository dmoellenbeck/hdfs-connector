/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.filesystem;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.mule.modules.hdfs.filesystem.dto.FileSystemStatus;
import org.mule.modules.hdfs.filesystem.exception.ConnectionRefused;
import org.mule.modules.hdfs.filesystem.exception.FileNotFound;
import org.mule.modules.hdfs.filesystem.exception.RuntimeIO;
import org.mule.modules.hdfs.filesystem.read.DataChunksConsumer;
import org.mule.modules.hdfs.filesystem.read.DataChunksReader;
import org.mule.modules.hdfs.filesystem.read.consumer.ConsumerStartingAtSpecificPosition;
import org.mule.modules.hdfs.filesystem.read.reader.ReaderStartingAtSpecificPosition;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.util.Objects;

/**
 * @author MuleSoft, Inc.
 */
public class HdfsFileSystem implements MuleFileSystem {

    private static final int DEFAULT_BUFFER_SIZE = 4096;
    private FileSystem fileSystem;

    public HdfsFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    @Override
    public DataChunksConsumer openConsumer(URI uri, long startPosition, int bufferSize) {
        try {
            Objects.requireNonNull(uri, "URI can not be null.");
            int positiveBufferSize = (bufferSize > 0) ? bufferSize : DEFAULT_BUFFER_SIZE;
            return new ConsumerStartingAtSpecificPosition(startPosition, positiveBufferSize, fileSystem.open(new Path(uri), positiveBufferSize));
        } catch (FileNotFoundException e) {
            throw new FileNotFound("File does not exist.", e);
        } catch (ConnectException e) {
            throw new ConnectionRefused("Unable to establish connection with server.", e);
        } catch (IOException e) {
            throw new RuntimeIO("Unexpected IO error occurred.", e);
        }
    }

    @Override
    public DataChunksReader openReader(URI uri, long startPosition, int bufferSize) {
        try {
            Objects.requireNonNull(uri, "URI can not be null.");
            int positiveBufferSize = (bufferSize > 0) ? bufferSize : DEFAULT_BUFFER_SIZE;
            return new ReaderStartingAtSpecificPosition(startPosition, positiveBufferSize, fileSystem.open(new Path(uri), positiveBufferSize));
        } catch (FileNotFoundException e) {
            throw new FileNotFound("File does not exist.", e);
        } catch (ConnectException e) {
            throw new ConnectionRefused("Unable to establish connection with server.", e);
        } catch (IOException e) {
            throw new RuntimeIO("Unexpected IO error occurred.", e);
        }
    }

    @Override
    public FileSystemStatus fileSystemStatus() {
        try {
            FsStatus fsStatus = fileSystem.getStatus();
            return new FileSystemStatus(fsStatus.getCapacity(), fsStatus.getUsed(), fsStatus.getRemaining());
        } catch (ConnectException e) {
            throw new ConnectionRefused("Unable to establish connection with server.", e);
        } catch (IOException e) {
            throw new RuntimeIO("Unexpected IO error occurred.", e);
        }
    }
}
