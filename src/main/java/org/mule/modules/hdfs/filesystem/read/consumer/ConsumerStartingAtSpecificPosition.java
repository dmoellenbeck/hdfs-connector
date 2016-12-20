/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.filesystem.read.consumer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.mule.modules.hdfs.filesystem.dto.DataChunk;
import org.mule.modules.hdfs.filesystem.exception.*;
import org.mule.modules.hdfs.filesystem.read.DataChunksConsumer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * @author MuleSoft, Inc.
 */
public class ConsumerStartingAtSpecificPosition implements DataChunksConsumer {

    private static final int DEFAULT_BUFFER_SIZE = 4096;
    private long startPosition;
    private int chunkSize;
    private FSDataInputStream sourceDataStream;
    private boolean closed;
    private Lock mutex;
    private volatile boolean consuming;
    private volatile boolean running;

    public ConsumerStartingAtSpecificPosition(long startPosition, int chunkSize, FSDataInputStream sourceDataStream) {
        Objects.requireNonNull(sourceDataStream, "Source data stream can not be null");
        this.startPosition = startPosition;
        this.chunkSize = chunkSize;
        this.sourceDataStream = sourceDataStream;
        this.closed = false;
        this.mutex = new ReentrantLock();
        this.running = true;
    }

    @Override
    public void consume(Consumer<DataChunk> consumer) {
        if (isClosed()) {
            throw new ConsumerClosed("Consumer is closed");
        }
        try {
            consuming = true;
            if (!isClosed()) {
                int positiveBufferSize = (chunkSize > 0) ? chunkSize : DEFAULT_BUFFER_SIZE;
                long positiveStartPosition = (startPosition > 0) ? startPosition : 0;
                if (positiveStartPosition > sourceDataStream.available()) {
                    throw new UnableToSeekToPosition("Cannot start reading after EOF.", positiveStartPosition, null);
                }
                sourceDataStream.seek(positiveStartPosition);
                int readBytes = 0;
                ByteBuffer byteBuffer = ByteBuffer.allocate(positiveBufferSize);
                while (running && readBytes >= 0) {
                    long positionInStream = sourceDataStream.getPos();
                    readBytes = sourceDataStream.read(byteBuffer);
                    byteBuffer.flip();
                    byte[] chunkData = new byte[byteBuffer.remaining()];
                    byteBuffer.get(chunkData);
                    byteBuffer.clear();
                    DataChunk readChunck = new DataChunk(positionInStream, readBytes, chunkData);
                    consumer.accept(readChunck);
                }
            }
        } catch (IOException e) {
            throw new ConnectionLost("Connection unexpectedly closed.", e);
        } finally {
            close(sourceDataStream);
        }
    }

    private boolean isClosed() {
        mutex.lock();
        try {
            return closed;
        } finally {
            mutex.unlock();
        }
    }

    private void close(FSDataInputStream sourceDataStream) {
        mutex.lock();
        try {
            if (!isClosed()) {
                closed = true;
                try {
                    sourceDataStream.close();
                } catch (IOException e) {
                    throw new ConnectionLost("Unable to close stream.", e);
                }
            }
        } finally {
            mutex.unlock();
        }
    }

    @Override
    public void close() {
        running = false;
        if (isClosed()) {
            throw new ConsumerAlreadyClosed("This consumer was already closed.");
        }
        if (!consuming) {
            close(sourceDataStream);
        }
    }
}
