/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.filesystem;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.mule.modules.hdfs.filesystem.dto.DataChunk;
import org.mule.modules.hdfs.filesystem.dto.FileSystemStatus;
import org.mule.modules.hdfs.filesystem.exception.ConnectionRefused;
import org.mule.modules.hdfs.filesystem.exception.FileNotFound;
import org.mule.modules.hdfs.filesystem.exception.RuntimeIO;
import org.mule.modules.hdfs.filesystem.exception.UnableToSeekToPosition;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * @author MuleSoft, Inc.
 */
public class HdfsFileSystem implements MuleFileSystem {

    public static final int DEFAULT_BUFFER_SIZE = 4096;
    private FileSystem fileSystem;

    public HdfsFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    @Override
    public void consume(URI uri, long startPosition, int bufferSize, Consumer<DataChunk> contentConsumer) {
        FSDataInputStream sourceDataStream = null;
        try {
            Objects.requireNonNull(uri, "URI can not be null.");
            int positiveBufferSize = (bufferSize > 0) ? bufferSize : DEFAULT_BUFFER_SIZE;
            long positiveStartPosition = (startPosition > 0) ? startPosition : 0;
            sourceDataStream = fileSystem.open(new Path(uri), positiveBufferSize);
            if (positiveStartPosition > sourceDataStream.available()) {
                throw new UnableToSeekToPosition("Cannot start reading after EOF.", positiveStartPosition, null);
            }
            sourceDataStream.seek(positiveStartPosition);
            int readBytes = 0;
            do {
                long positionInStream = sourceDataStream.getPos();
                ByteBuffer byteBuffer = ByteBuffer.allocate(positiveBufferSize);
                readBytes = sourceDataStream.read(byteBuffer);
                byteBuffer.flip();
                byte[] chunkData = new byte[byteBuffer.remaining()];
                byteBuffer.get(chunkData);
                byteBuffer.clear();
                DataChunk readChunck = new DataChunk(positionInStream, readBytes, chunkData);
                contentConsumer.accept(readChunck);
            } while (readBytes > 0);
        } catch (FileNotFoundException e) {
            throw new FileNotFound("File does not exist.", e);
        } catch (ConnectException e) {
            throw new ConnectionRefused("Unable to establish connection with server.", e);
        } catch (IOException e) {
            throw new RuntimeIO("Unexpected IO error occurred.", e);
        } finally {
            close(sourceDataStream);
        }
    }

    private void close(FSDataInputStream sourceDataStream) {
        if (sourceDataStream != null) {
            try {
                sourceDataStream.close();
            } catch (IOException e) {
                throw new RuntimeIO("Unable to close stream.", e);
            }
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
