/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.filesystem.read.reader;

import org.apache.hadoop.fs.FSDataInputStream;
import org.mule.modules.hdfs.filesystem.dto.DataChunk;
import org.mule.modules.hdfs.filesystem.exception.*;
import org.mule.modules.hdfs.filesystem.read.DataChunksReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * @author MuleSoft, Inc.
 */
public class ReaderStartingAtSpecificPosition implements DataChunksReader {

    private static final Logger logger = LoggerFactory.getLogger(ReaderStartingAtSpecificPosition.class);
    private static final int DEFAULT_BUFFER_SIZE = 4096;
    private final ByteBuffer byteBuffer;
    private boolean closed;
    private long startPosition;
    private int chunkSize;
    private FSDataInputStream sourceDataStream;

    public ReaderStartingAtSpecificPosition(long startPosition, int chunkSize, FSDataInputStream sourceDataStream) {
        Objects.requireNonNull(sourceDataStream, "Source data stream can not be null");
        this.startPosition = (startPosition > 0) ? startPosition : 0;
        this.chunkSize = (chunkSize > 0) ? chunkSize : DEFAULT_BUFFER_SIZE;
        this.sourceDataStream = sourceDataStream;
        this.closed = false;
        seekToStartPosition();
        byteBuffer = ByteBuffer.allocate(this.chunkSize);
    }

    private void seekToStartPosition() {
        try {
            if (startPosition > sourceDataStream.available()) {
                throw new UnableToSeekToPosition("Cannot start reading after EOF.", startPosition, null);
            }
            sourceDataStream.seek(startPosition);
        } catch (IOException e) {
            logger.error("Connection unexpectedly closed.", e);
            throw new ConnectionLost("Connection unexpectedly closed.", e);
        }
    }

    @Override
    public DataChunk nextChunk() {
        throwExceptionIfReaderClosed();
        throwExceptionIfNoMoreDataAvailable();
        try {
            long positionInStream = sourceDataStream.getPos();
            int readBytes = sourceDataStream.read(byteBuffer);
            byteBuffer.flip();
            byte[] chunkData = new byte[byteBuffer.remaining()];
            byteBuffer.get(chunkData);
            byteBuffer.clear();
            return new DataChunk(positionInStream, readBytes, chunkData);
        } catch (IOException e) {
            logger.error("Connection unexpectedly closed.", e);
            throw new ConnectionLost("Connection unexpectedly closed.", e);
        }
    }

    private void throwExceptionIfNoMoreDataAvailable() {
        if (!hasNext()) {
            throw new NoMoreDataAvailable("No more data left to be page.");
        }
    }

    private void throwExceptionIfReaderClosed() {
        if (isClosed()) {
            throw new ReaderClosed("Reader is closed");
        }
    }

    private boolean isClosed() {
        return closed;
    }

    @Override
    public boolean hasNext() {
        throwExceptionIfReaderClosed();
        try {
            return sourceDataStream.available() > 0;
        } catch (IOException e) {
            logger.error("Connection unexpectedly closed.", e);
            throw new ConnectionLost("Connection unexpectedly closed.", e);
        }
    }

    private void close(FSDataInputStream sourceDataStream) {
        closed = true;
        try {
            sourceDataStream.close();
        } catch (IOException e) {
            logger.error("Connection unexpectedly closed.", e);
            throw new ConnectionLost("Unable to close stream.", e);
        }
    }

    @Override
    public void close() {
        if (isClosed()) {
            throw new ReaderAlreadyClosed("This reader was already closed.");
        }
        close(sourceDataStream);
    }
}
