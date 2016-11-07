/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.extension.source;

import org.mule.modules.hdfs.HDFSConnector;
import org.mule.modules.hdfs.exception.UnableToStopSource;
import org.mule.modules.hdfs.filesystem.HdfsConnection;
import org.mule.runtime.api.message.Attributes;
import org.mule.runtime.extension.api.annotation.Parameter;
import org.mule.runtime.extension.api.annotation.param.Connection;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.runtime.source.Source;
import org.mule.runtime.extension.api.runtime.source.SourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Read the content of a file designated by its path and streams it to the rest of the flow, while adding the path metadata in the following inbound properties:
 * <ul>
 * <li>{@link HDFSConnector#HDFS_PATH_EXISTS}: a boolean set to true if the path exists</li>
 * <li>{@link HDFSConnector#HDFS_CONTENT_SUMMARY}: an instance of {@link ContentSummary} if the path exists.</li>
 * <li>{@link HDFSConnector#HDFS_FILE_STATUS}: an instance of {@link FileStatus} if the path exists.</li>
 * <li>{@link HDFSConnector#HDFS_FILE_CHECKSUM}: an instance of {@link FileChecksum} if the path exists, is a file and has a checksum.</li>
 * </ul>
 * 
 */
public class ReadSource extends Source<InputStream, Attributes> {

    private static final Logger logger = LoggerFactory.getLogger(ReadSource.class);

    private ExecutorService executor;

    /**
     * 
     * @param path
     *            the path of the file to read.
     */
    @Parameter
    private String path;

    /**
     * 
     * @param bufferSize
     *            the buffer size to use when reading the file.
     */
    @Parameter
    @Optional(defaultValue = "4096")
    private int bufferSize;

    @Connection
    private HdfsConnection connection;

    @Override
    public void start() {
        Runnable runnable = this.createRunnable(this.sourceContext);
        executor = Executors.newScheduledThreadPool(1);
        ((ScheduledExecutorService) executor).scheduleAtFixedRate(runnable, 0, 5000L, TimeUnit.MILLISECONDS);
    }

    private Runnable createRunnable(final SourceContext sourceContext) {
        return () -> {
//                SourceUtils.executeOperation(connection,
//                        sourceContext.getExceptionCallback(),
//                        hdfsConnector -> hdfsConnector.read(path, bufferSize, sourceContext.getMessageHandler()));
            };
    }

    @Override
    public void stop() {
        executor.shutdownNow();
        try {
            executor.awaitTermination(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new UnableToStopSource("Something happened while stopping source.", e);
        }
    }

}
