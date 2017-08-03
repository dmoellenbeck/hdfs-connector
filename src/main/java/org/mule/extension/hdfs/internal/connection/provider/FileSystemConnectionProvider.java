package org.mule.extension.hdfs.internal.connection.provider;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.mule.extension.hdfs.internal.connection.FileSystemConnection;
import org.mule.extension.hdfs.internal.connection.HdfsConnection;
import org.mule.extension.hdfs.internal.service.exception.UnableToCloseConnection;
import org.mule.runtime.api.connection.ConnectionValidationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FileSystemConnectionProvider {

    private static final Logger logger = LoggerFactory.getLogger(FileSystemConnectionProvider.class);

    public void disconnect(HdfsConnection conn) {
        FileSystemConnection fsConn = (FileSystemConnection) conn;
        try {
            if (fsConn != null && fsConn.getFileSystem() != null) {
                fsConn.getFileSystem()
                        .close();
            }
        } catch (Exception e) {
            logger.error("Unable to end connection.", e);
            throw new UnableToCloseConnection("Unable to end connection.", e);
        } finally {
            conn = null;
        }
    }

    public ConnectionValidationResult validate(HdfsConnection conn) {
        FileSystemConnection fsConn = (FileSystemConnection) conn;
        try {
            if (fsConn != null && fsConn.getFileSystem() != null) {
                // ignore the result: an exception will be thrown in case of issue
                fsConn.getFileSystem()
                        .listStatus(new Path("/"));
                return ConnectionValidationResult.success();
            }
        } catch (final IOException e) {
            logger.error("Failed to connect to HDFS", e);
        }
        return ConnectionValidationResult.failure("Connection is no longer valid", null);
    }

}
