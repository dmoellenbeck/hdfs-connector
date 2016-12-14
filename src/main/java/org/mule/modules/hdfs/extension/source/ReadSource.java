/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.extension.source;

import org.mule.modules.hdfs.exception.UnableToStopSource;
import org.mule.modules.hdfs.filesystem.HdfsConnection;
import org.mule.modules.hdfs.filesystem.HdfsFileSystemProvider;
import org.mule.modules.hdfs.filesystem.MuleFileSystem;
import org.mule.modules.hdfs.filesystem.dto.DataChunk;
import org.mule.modules.hdfs.filesystem.exception.RuntimeIO;
import org.mule.runtime.api.message.Attributes;
import org.mule.runtime.extension.api.annotation.param.Connection;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;
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

    private ScheduledExecutorService scheduledExecutorService;

    /**
     * The path of the file to pool the content from.
     */
    @Parameter
    private String path;

    @Parameter
    @Optional(defaultValue = "0")
    private Long startPosition;

    @Parameter
    @Optional(defaultValue = "4096")
    private Integer blockSize;

    @Parameter
    @Optional(defaultValue = "5000")
    @DisplayName("Polling frequency (ms)")
    private Long pollingFrequency;

    @Connection
    private HdfsConnection hdfsConnection;

    @Inject
    private HdfsFileSystemProvider fileSystemProvider;

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

    public Integer getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(Integer blockSize) {
        this.blockSize = blockSize;
    }

    public Long getPollingFrequency() {
        return pollingFrequency;
    }

    public void setPollingFrequency(Long pollingFrequency) {
        this.pollingFrequency = pollingFrequency;
    }

    public HdfsConnection getHdfsConnection() {
        return hdfsConnection;
    }

    public void setHdfsConnection(HdfsConnection hdfsConnection) {
        this.hdfsConnection = hdfsConnection;
    }

    public HdfsFileSystemProvider getFileSystemProvider() {
        return fileSystemProvider;
    }

    public void setFileSystemProvider(HdfsFileSystemProvider fileSystemProvider) {
        this.fileSystemProvider = fileSystemProvider;
    }

    @Override
    public void onStart(SourceCallback<DataChunk, Attributes> sourceCallback) {
        URI uriToFileToReadFrom = uriForFileToReadFrom();
        Consumer<DataChunk> contentConsumer = item -> {
            sourceCallback.handle(Result.<DataChunk, Attributes>builder().output(item).build());
        };
        Consumer<RuntimeIO> exceptionConsumer = sourceCallback::onSourceException;
        Runnable runnable = createRunnable(uriToFileToReadFrom, startPosition, blockSize, contentConsumer, exceptionConsumer);
        startPollingTheContentFromFile(runnable);
    }

    private void startPollingTheContentFromFile(Runnable runnable) {
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(runnable, 0, pollingFrequency, TimeUnit.MILLISECONDS);
    }

    private URI uriForFileToReadFrom() {
        return URI.create(path);
    }

    private MuleFileSystem fileSystem() {
        return fileSystemProvider.fileSystem(hdfsConnection);
    }

    private Runnable createRunnable(URI uriOfTheFileToReadFrom, long startPosition, int blockSize,
            Consumer<DataChunk> contentConsumer, Consumer<RuntimeIO> exceptionConsumer) {
        return () -> {
            try {
                fileSystem().consume(uriOfTheFileToReadFrom, startPosition, blockSize, contentConsumer);
            } catch (RuntimeIO exception) {
                exceptionConsumer.accept(exception);
            }
        };
    }

    @Override
    public void onStop() {
        scheduledExecutorService.shutdownNow();
        try {
            scheduledExecutorService.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Something happened while stopping source.", e);
            throw new UnableToStopSource("Something happened while stopping source.", e);
        }
    }

}
