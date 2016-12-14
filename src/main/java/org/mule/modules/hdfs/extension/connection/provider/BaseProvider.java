/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.hdfs.extension.connection.provider;

import org.mule.modules.hdfs.filesystem.HdfsConnection;
import org.mule.modules.hdfs.filesystem.HdfsFileSystemProvider;
import org.mule.modules.hdfs.filesystem.exception.AuthenticationFailed;
import org.mule.modules.hdfs.filesystem.exception.ConnectionRefused;
import org.mule.modules.hdfs.filesystem.exception.RuntimeIO;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.connection.ConnectionExceptionCode;
import org.mule.runtime.api.connection.ConnectionProvider;
import org.mule.runtime.api.connection.ConnectionValidationResult;

import javax.inject.Inject;

/**
 * @author MuleSoft, Inc.
 */
public abstract class BaseProvider implements ConnectionProvider<HdfsConnection> {

    @Inject
    private HdfsFileSystemProvider hdfsFileSystemProvider;

    protected void checkConnect(HdfsConnection hdfsConnection) throws ConnectionException {
        try {
            checkFileSystem(hdfsConnection);
        } catch (RuntimeIO e) {
            throw new ConnectionException("Unable to establish connection with server", e);
        }
    }

    protected void checkFileSystem(HdfsConnection hdfsConnection) {
        hdfsFileSystemProvider.fileSystem(hdfsConnection)
                .fileSystemStatus();
    }

    @Override
    public ConnectionValidationResult validate(HdfsConnection connection) {
        try {
            checkFileSystem(connection);
            return ConnectionValidationResult.success();
        } catch (AuthenticationFailed e) {
            return ConnectionValidationResult.failure("Failed to authenticate against server.", ConnectionExceptionCode.INCORRECT_CREDENTIALS, e);
        } catch (ConnectionRefused e) {
            return ConnectionValidationResult.failure("Server seems to be dead.", ConnectionExceptionCode.CANNOT_REACH, e);
        } catch (RuntimeIO e) {
            return ConnectionValidationResult.failure("Unable to establish connection with server", ConnectionExceptionCode.UNKNOWN, e);
        }
    }
}
