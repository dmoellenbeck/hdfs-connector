/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.extension.connection.provider;

import org.mule.modules.hdfs.filesystem.HdfsConnection;
import org.mule.modules.hdfs.filesystem.HdfsFileSystemProvider;
import org.mule.modules.hdfs.filesystem.exception.RuntimeIO;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.connection.ConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author MuleSoft, Inc.
 */
public abstract class BaseProvider implements ConnectionProvider<HdfsConnection> {

    private static final Logger logger = LoggerFactory.getLogger(BaseProvider.class);

    public void checkConnect(HdfsConnection hdfsConnection) throws ConnectionException {
        try {
            checkFileSystem(hdfsConnection);
        } catch (RuntimeIO e) {
            throw new ConnectionException("Unable to establish connection with server", e);
        }
    }

    protected void checkFileSystem(HdfsConnection hdfsConnection) {
        HdfsFileSystemProvider hdfsFileSystemProvider = new HdfsFileSystemProvider();
        hdfsFileSystemProvider.fileSystem(hdfsConnection)
                .fileSystemStatus();
    }

}
