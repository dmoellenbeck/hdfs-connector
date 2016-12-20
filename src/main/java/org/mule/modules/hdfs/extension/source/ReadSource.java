/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.extension.source;

import org.mule.modules.hdfs.exception.UnableToStopSource;
import org.mule.modules.hdfs.extension.dto.ReadParameters;
import org.mule.modules.hdfs.filesystem.HdfsConnection;
import org.mule.modules.hdfs.filesystem.HdfsFileSystemProvider;
import org.mule.modules.hdfs.filesystem.MuleFileSystem;
import org.mule.modules.hdfs.filesystem.dto.DataChunk;
import org.mule.modules.hdfs.filesystem.exception.RuntimeIO;
import org.mule.modules.hdfs.filesystem.read.DataChunksConsumer;
import org.mule.runtime.api.message.Attributes;
import org.mule.runtime.extension.api.annotation.param.Connection;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.param.ParameterGroup;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.runtime.operation.Result;
import org.mule.runtime.extension.api.runtime.source.Source;
import org.mule.runtime.extension.api.runtime.source.SourceCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Pools the content of the given path every 5 seconds.
 *
 * @author MuleSoft, Inc.
 */
public class ReadSource extends Source<DataChunk, Attributes> {

    private static final Logger logger = LoggerFactory.getLogger(ReadSource.class);

    @Parameter
    @Optional(defaultValue = "5000")
    @DisplayName("Polling frequency (ms)")
    private Long pollingFrequency;
    @ParameterGroup("Read")
    private ReadParameters readParameters;
    @Connection
    private HdfsConnection hdfsConnection;
    @Inject
    private HdfsFileSystemProvider fileSystemProvider;
    private ScheduledExecutorService scheduledExecutorService;
    private DataChunksConsumerTask chunksConsumerTask;

    public Long getPollingFrequency() {
        return pollingFrequency;
    }

    public void setPollingFrequency(Long pollingFrequency) {
        this.pollingFrequency = pollingFrequency;
    }

    public ReadParameters getReadParameters() {
        return readParameters;
    }

    public void setReadParameters(ReadParameters readParameters) {
        this.readParameters = readParameters;
    }

    public HdfsConnection getHdfsConnection() {
        return hdfsConnection;
    }

    public void setHdfsConnection(HdfsConnection hdfsConnection) {
        this.hdfsConnection = hdfsConnection;
    }

    @Override
    public void onStart(SourceCallback<DataChunk, Attributes> sourceCallback) {
        URI uriToFileToReadFrom = uriForFileToReadFrom();
        Consumer<DataChunk> contentConsumer = item -> {
            sourceCallback.handle(Result.<DataChunk, Attributes>builder().output(item).build());
        };
        Consumer<RuntimeIO> exceptionConsumer = sourceCallback::onSourceException;
        chunksConsumerTask = new DataChunksConsumerTask(uriToFileToReadFrom, readParameters.getStartPosition(), readParameters.getChunkSize(), contentConsumer, exceptionConsumer);
        startPollingTheContentFromFile(chunksConsumerTask);
    }

    private void startPollingTheContentFromFile(Runnable runnable) {
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(runnable, 0, pollingFrequency, TimeUnit.MILLISECONDS);
    }

    private URI uriForFileToReadFrom() {
        return URI.create(readParameters.getPath());
    }

    private MuleFileSystem fileSystem() {
        return fileSystemProvider.fileSystem(hdfsConnection);
    }

    @Override
    public void onStop() {
        try {
            chunksConsumerTask.stopTask();
        } finally {
            stopExecutor();
        }
    }

    private void stopExecutor() {
        scheduledExecutorService.shutdownNow();
        try {
            scheduledExecutorService.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Something happened while stopping source thread.", e);
            throw new UnableToStopSource("Something happened while stopping source thread.", e);
        }
    }

    private class DataChunksConsumerTask implements Runnable {

        private DataChunksConsumer chunksConsumer;
        private Consumer<DataChunk> contentConsumer;
        private Consumer<RuntimeIO> exceptionConsumer;
        private URI uriOfTheFileToReadFrom;
        private long startPosition;
        private int blockSize;

        public DataChunksConsumerTask(URI uriOfTheFileToReadFrom, long startPosition, int blockSize, Consumer<DataChunk> contentConsumer,
                Consumer<RuntimeIO> exceptionConsumer) {
            this.uriOfTheFileToReadFrom = uriOfTheFileToReadFrom;
            this.startPosition = startPosition;
            this.blockSize = blockSize;
            this.contentConsumer = contentConsumer;
            this.exceptionConsumer = exceptionConsumer;
        }

        @Override
        public void run() {
            try {
                chunksConsumer = fileSystem().openConsumer(uriOfTheFileToReadFrom, startPosition, blockSize);
                chunksConsumer.consume(contentConsumer);
            } catch (RuntimeIO e) {
                logger.error("Something bad happened while consuming data chunks.", e);
                exceptionConsumer.accept(e);
            }
        }

        private void stopTask() {
            if (chunksConsumer != null) {
                chunksConsumer.close();
            }
        }
    }

}
